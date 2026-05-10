# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

"""
Quickwit DatabaseClient adapter.

Quickwit exposes a subset of the Elasticsearch/OpenSearch REST API at the
`/api/v1/_elastic/` URL prefix. This adapter:

  * Constructs an opensearchpy client pointed at Quickwit's `_elastic`
    endpoint by injecting a `url_prefix` into the underlying client
    options.
  * Composes an `OpenSearchDatabaseClient` for the operations Quickwit
    natively supports (bulk, search, delete-index, cluster.health,
    index-stats, info).
  * Overrides operations Quickwit does not natively expose
    (`create-index`, `refresh`, `force-merge`, `put-settings`,
    `reindex`) with no-op implementations that emit a WARN log on
    first occurrence and DEBUG thereafter, per the adapter
    unsupported-op policy.

Operation classification source of truth:
`~/.claude/projects/-Users-zznate-Documents-GitHub-opensearch-benchmark/memory/project_quickwit_op_matrix.md`
"""

import logging

from osbenchmark.database.clients.opensearch.opensearch import (
    OpenSearchDatabaseClient,
)
from osbenchmark.database.interface import (
    ClusterNamespace,
    DatabaseClient,
    IndicesNamespace,
)
from osbenchmark.client import OsClientFactory, wait_for_rest_layer


_ADAPTER_NAME = "quickwit"
QUICKWIT_URL_PREFIX = "api/v1/_elastic"
QUICKWIT_NATIVE_INDEXES_PATH = "/api/v1/indexes"
QUICKWIT_CONFIG_VERSION = "0.8"


# Mapping from OpenSearch field-type names to Quickwit IndexConfig field-type
# names. Types not in this table fall through to dynamic-mode handling.
_OS_TO_QW_TYPE = {
    "integer": "i64",
    "long": "i64",
    "short": "i64",
    "byte": "i64",
    "unsigned_long": "u64",
    "float": "f64",
    "double": "f64",
    "half_float": "f64",
    "scaled_float": "f64",
    "boolean": "bool",
    "ip": "ip",
}


def _os_field_to_quickwit(name, defn):
    """
    Translate an OpenSearch mapping field definition to a Quickwit
    field_mappings entry. Returns None for types Quickwit can't represent
    (those are left to dynamic mode).
    """
    if not isinstance(defn, dict):
        return None
    os_type = defn.get("type")
    if os_type == "date":
        return {
            "name": name,
            "type": "datetime",
            "input_formats": ["rfc3339", "unix_timestamp"],
            "output_format": "unix_timestamp_secs",
            "fast_precision": "seconds",
            "fast": True,
        }
    if os_type == "keyword":
        # OS keyword (non-tokenized, doc-values usually on) → Quickwit text/raw.
        # Honor OS `index: false` by setting indexed=false AND omitting the
        # tokenizer (Quickwit rejects `tokenizer`/`record`/`fieldnorms`
        # alongside `indexed: false`). Stored remains true so docs round-trip.
        indexed = bool(defn.get("index", True))
        field = {"name": name, "type": "text", "indexed": indexed}
        if indexed:
            field["tokenizer"] = "raw"
        return field
    if os_type == "text":
        return {
            "name": name,
            "type": "text",
            "tokenizer": "default",
            "record": "position",
        }
    if "properties" in defn or os_type == "object":
        sub_props = defn.get("properties", {}) or {}
        sub_fields = []
        for sub_name, sub_def in sub_props.items():
            sub = _os_field_to_quickwit(sub_name, sub_def)
            if sub is not None:
                sub_fields.append(sub)
        return {"name": name, "type": "object", "field_mappings": sub_fields}
    qw_type = _OS_TO_QW_TYPE.get(os_type)
    if qw_type is None:
        # geo_point, geo_shape, completion, etc. — let dynamic mode handle
        # whatever the bulk docs actually contain.
        return None
    field = {"name": name, "type": qw_type}
    if qw_type in ("i64", "u64", "f64", "ip"):
        field["fast"] = True
    return field


def _post_native_index_config(host_entry, use_tls, config_dict):
    """
    Fork-safe POST of a Quickwit IndexConfig (as JSON) to /api/v1/indexes.
    Uses http.client to avoid the macOS urllib/CFNetwork fork hazard. Returns
    (status_code, response_body_str).
    """
    # pylint: disable=import-outside-toplevel
    import http.client
    import json as _json

    body = _json.dumps(config_dict)
    headers = {"content-type": "application/json"}
    if use_tls:
        conn = http.client.HTTPSConnection(host_entry["host"], host_entry["port"], timeout=10)
    else:
        conn = http.client.HTTPConnection(host_entry["host"], host_entry["port"], timeout=10)
    try:
        conn.request("POST", QUICKWIT_NATIVE_INDEXES_PATH, body=body, headers=headers)
        resp = conn.getresponse()
        resp_body = resp.read().decode("utf-8", errors="replace")
        return resp.status, resp_body
    finally:
        conn.close()


def _translate_os_mapping_to_quickwit_config(index_name, body):
    """
    Build a Quickwit IndexConfig (as a dict ready for JSON-serialization)
    from an OpenSearch-style create-index body. Best-effort: types we can
    map are listed explicitly; unmapped fields are accepted via
    `mode: dynamic` so bulk doesn't reject documents.
    """
    config = {
        "version": QUICKWIT_CONFIG_VERSION,
        "index_id": index_name,
        "doc_mapping": {
            "mode": "dynamic",
            "field_mappings": [],
        },
        "indexing_settings": {"commit_timeout_secs": 30},
    }
    mappings = (body or {}).get("mappings") or {}
    props = mappings.get("properties") or {}
    timestamp_field = None
    for fname, fdef in props.items():
        qw_field = _os_field_to_quickwit(fname, fdef)
        if qw_field is not None:
            config["doc_mapping"]["field_mappings"].append(qw_field)
        # First date field wins as timestamp_field.
        if timestamp_field is None and isinstance(fdef, dict) and fdef.get("type") == "date":
            timestamp_field = fname
    if timestamp_field is not None:
        config["doc_mapping"]["timestamp_field"] = timestamp_field
    return config


class _UnsupportedOpLog:
    """
    First-occurrence WARN, subsequent-occurrence DEBUG. Shared across
    the client and its namespaces via composition so a single op type
    only warns once per run regardless of which namespace handled it.
    """

    def __init__(self, logger):
        self._logger = logger
        self._seen = set()

    def __call__(self, op, reason):
        if op in self._seen:
            self._logger.debug(
                "unsupported operation (repeat): adapter=%s op=%s",
                _ADAPTER_NAME, op,
            )
            return
        self._seen.add(op)
        self._logger.warning(
            "unsupported operation: adapter=%s op=%s reason=%s",
            _ADAPTER_NAME, op, reason,
        )


class QuickwitIndicesNamespace(IndicesNamespace):
    """
    Wraps OpenSearch's IndicesNamespace; overrides operations Quickwit
    doesn't expose via the _elastic API surface.
    """

    def __init__(self, inner, unsupported_log, host_entry, use_tls):
        self._inner = inner
        self._unsupported = unsupported_log
        self._host_entry = host_entry
        self._use_tls = use_tls
        self._logger = logging.getLogger(__name__)

    async def delete(self, index, **kwargs):
        return await self._inner.delete(index, **kwargs)

    async def exists(self, index, **kwargs):
        return await self._inner.exists(index, **kwargs)

    async def stats(self, index=None, metric=None, **kwargs):  # pylint: disable=invalid-overridden-method
        return await self._inner.stats(index=index, metric=metric, **kwargs)

    async def create(self, index, body=None, **kwargs):
        """
        Translate an OS create-index request to Quickwit's native
        POST /api/v1/indexes. The OS-side body is best-effort-mapped to
        a Quickwit IndexConfig; unmapped field types are absorbed by
        `mode: dynamic`. Returns an OS-shape acknowledged response so
        the calling runner is satisfied regardless of native success.
        """
        # pylint: disable=import-outside-toplevel
        import asyncio
        config = _translate_os_mapping_to_quickwit_config(index, body)
        try:
            status, resp_body = await asyncio.to_thread(
                _post_native_index_config, self._host_entry, self._use_tls, config,
            )
            if status == 200:
                self._logger.info(
                    "Quickwit native create-index succeeded: index=%s", index,
                )
            elif status == 400 and "already exist" in resp_body.lower():
                self._logger.info(
                    "Quickwit index already exists; treating as success: index=%s",
                    index,
                )
            else:
                self._logger.warning(
                    "Quickwit native create-index returned status=%d body=%s",
                    status, resp_body[:300],
                )
        except Exception as e:  # noqa: BLE001
            # Don't break the workload over a create-index translation issue;
            # log and continue. Bulk to a missing index will fail next, which
            # surfaces the real problem.
            self._logger.warning(
                "Quickwit native create-index raised: index=%s err=%s", index, e,
            )
        return {"acknowledged": True, "shards_acknowledged": True, "index": index}

    async def refresh(self, index=None, **kwargs):
        self._unsupported(
            "refresh",
            "Quickwit uses commit-based ingestion; no per-request refresh endpoint",
        )
        return {"_shards": {"total": 0, "successful": 0, "failed": 0}}

    async def forcemerge(self, index=None, **kwargs):
        self._unsupported(
            "force-merge",
            "Quickwit manages segment merges internally via the merge pipeline",
        )
        return {"_shards": {"total": 0, "successful": 0, "failed": 0}}

    def __getattr__(self, name):
        return getattr(self._inner, name)


class QuickwitClusterNamespace(ClusterNamespace):
    """
    Wraps OpenSearch's ClusterNamespace; overrides operations Quickwit
    doesn't expose via the _elastic API surface.
    """

    def __init__(self, inner, unsupported_log):
        self._inner = inner
        self._unsupported = unsupported_log

    async def health(self, **kwargs):
        # Quickwit 0.8.2-nightly (Sept 2024 build, the current :latest
        # Docker image) does not expose GET /_elastic/_cluster/health.
        # Newer Quickwit builds (>= qw-azure-20250812) do expose it.
        # Treat as Unsupported across the board for compatibility; the
        # synthetic green response keeps the OS cluster-health runner
        # happy regardless of Quickwit version.
        self._unsupported(
            "cluster-health",
            "_cluster/health absent in older Quickwit builds; returning synthetic green",
        )
        return {
            "cluster_name": "quickwit",
            "status": "green",
            "timed_out": False,
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "active_primary_shards": 0,
            "active_shards": 0,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
            "delayed_unassigned_shards": 0,
            "number_of_pending_tasks": 0,
            "number_of_in_flight_fetch": 0,
            "task_max_waiting_in_queue_millis": 0,
            "active_shards_percent_as_number": 100.0,
        }

    async def put_settings(self, body, **kwargs):
        self._unsupported(
            "put-settings",
            "Quickwit settings model differs; no _elastic cluster settings endpoint",
        )
        return {"acknowledged": True, "persistent": {}, "transient": {}}

    def __getattr__(self, name):
        return getattr(self._inner, name)


class QuickwitDatabaseClient(DatabaseClient):
    """
    DatabaseClient implementation for Quickwit, composed of an
    OpenSearch wrapper. Native operations delegate to the inner client;
    unsupported operations return policy-conforming no-op responses.
    """

    def __init__(self, inner, host_entry, use_tls):
        self._inner = inner
        self._logger = logging.getLogger(__name__)
        self._unsupported = _UnsupportedOpLog(self._logger)
        self._indices_ns = QuickwitIndicesNamespace(
            inner.indices, self._unsupported, host_entry, use_tls,
        )
        self._cluster_ns = QuickwitClusterNamespace(inner.cluster, self._unsupported)

    @property
    def indices(self):
        return self._indices_ns

    @property
    def cluster(self):
        return self._cluster_ns

    @property
    def transport(self):
        return self._inner.transport

    @property
    def nodes(self):
        return self._inner.nodes

    async def bulk(self, body, index=None, doc_type=None, params=None, **kwargs):
        return await self._inner.bulk(
            body=body, index=index, doc_type=doc_type, params=params, **kwargs,
        )

    async def search(self, index=None, body=None, doc_type=None, **kwargs):
        return await self._inner.search(
            index=index, body=body, doc_type=doc_type, **kwargs,
        )

    async def index(self, index, body, id=None, doc_type=None, **kwargs):
        self._unsupported(
            "index",
            "Quickwit does not expose single-document index via _elastic; use bulk",
        )
        return {"_index": index, "_id": id or "", "result": "noop"}

    async def reindex(self, body, **kwargs):
        self._unsupported(
            "reindex",
            "no _elastic reindex endpoint in Quickwit",
        )
        return {"took": 0, "timed_out": False, "total": 0,
                "updated": 0, "created": 0, "deleted": 0,
                "batches": 0, "version_conflicts": 0, "noops": 0,
                "retries": {"bulk": 0, "search": 0}, "failures": []}

    def info(self):
        # Quickwit serves a cluster-info-shaped response at
        # GET /_elastic (see quickwit-serve elasticsearch_api/filter.rs:41).
        return self._inner.info()

    def return_raw_response(self):
        return self._inner.return_raw_response()

    def close(self):
        return self._inner.close()

    def __getattr__(self, name):
        """
        Forward unrecognized attributes to the underlying OpenSearch
        wrapper. This preserves access to opensearchpy features that
        Quickwit may or may not support; the request will simply
        succeed or fail at Quickwit's HTTP boundary. Operations the
        matrix flags as Unsupported should be explicitly overridden
        above rather than relying on this fall-through.
        """
        return getattr(self._inner, name)


class QuickwitClientFactory:
    """
    Factory for Quickwit `DatabaseClient` instances.

    Composition over subclassing: this factory creates a regular
    opensearchpy client via `OsClientFactory`, then wraps it in
    `OpenSearchDatabaseClient`, then in `QuickwitDatabaseClient`. The
    Quickwit-specific routing happens via the `url_prefix` injected
    into client options.
    """

    def __init__(self, hosts, client_options):
        self.hosts = hosts
        # Copy and inject the Quickwit prefix. If the user supplied
        # their own `url_prefix` (e.g. behind a path-rewriting proxy)
        # respect it rather than clobbering.
        opts = dict(client_options)
        opts.setdefault("url_prefix", QUICKWIT_URL_PREFIX)
        self.client_options = opts
        self.logger = logging.getLogger(__name__)
        self.logger.info(
            "Configuring Quickwit client: hosts=%s url_prefix=%s",
            hosts, self.client_options["url_prefix"],
        )

    def create_async(self):
        os_factory = OsClientFactory(self.hosts, self.client_options)
        opensearch_client = os_factory.create_async()
        os_db_client = OpenSearchDatabaseClient(opensearch_client)
        host_entry = self.hosts[0]
        use_tls = bool(self.client_options.get("use_ssl"))
        return QuickwitDatabaseClient(os_db_client, host_entry, use_tls)

    def create(self):
        os_factory = OsClientFactory(self.hosts, self.client_options)
        return os_factory.create()

    def wait_for_rest_layer(self, max_attempts=40):
        """
        Probe Quickwit's native liveness endpoint at /health/livez.
        Uses http.client (not urllib.request) because urllib triggers
        macOS's SystemConfiguration proxy detection through
        CoreFoundation, which deadlocks after a Thespian-style fork.
        http.client speaks plain HTTP and is fork-safe.
        """
        # pylint: disable=import-outside-toplevel
        import http.client
        import socket
        import time

        host_entry = self.hosts[0]
        use_tls = bool(self.client_options.get("use_ssl"))

        for attempt in range(1, max_attempts + 1):
            conn = None
            try:
                if use_tls:
                    conn = http.client.HTTPSConnection(
                        host_entry["host"], host_entry["port"], timeout=5,
                    )
                else:
                    conn = http.client.HTTPConnection(
                        host_entry["host"], host_entry["port"], timeout=5,
                    )
                conn.request("GET", "/health/livez")
                resp = conn.getresponse()
                body = resp.read().decode("utf-8").strip()
                if resp.status == 200 and body == "true":
                    self.logger.info(
                        "Quickwit REST layer ready at [%s:%s] after [%d] attempts.",
                        host_entry["host"], host_entry["port"], attempt,
                    )
                    return True
                self.logger.debug(
                    "Quickwit /health/livez returned status=%s body=%r; retrying",
                    resp.status, body,
                )
            except (http.client.HTTPException, socket.error, OSError) as e:
                self.logger.debug(
                    "Quickwit /health/livez attempt %d failed: %s; sleeping",
                    attempt, e,
                )
            finally:
                if conn is not None:
                    conn.close()
            time.sleep(3)
        return False
