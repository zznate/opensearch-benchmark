# Quickwit DatabaseClient adapter

A `DatabaseClient` implementation that lets OpenSearch Benchmark
workloads run against [Quickwit](https://quickwit.io/) via its
Elasticsearch-compatible (and thus mostly OpenSeach compatible) API. Built on top of the database abstraction layer introduced in PR #1023.

## What it does

Quickwit exposes a subset of the Elasticsearch/OpenSearch REST API
under the `/api/v1/_elastic/` URL prefix. The adapter wires OSB
runners through that surface by:

1. Constructing the underlying `opensearchpy` client with
   `url_prefix=api/v1/_elastic`, so calls like `client.search(...)`
   reach Quickwit's `_elastic` endpoints.
2. Composing an `OpenSearchDatabaseClient` underneath and overriding
   only the operations Quickwit can't satisfy via that surface.
3. Translating `create-index` to Quickwit's native
   `POST /api/v1/indexes` endpoint, mapping the OS-style schema to a
   Quickwit `IndexConfig` (best-effort, with `mode: dynamic` as the
   safety net).
4. No-opping operations that have no Quickwit equivalent
   (`refresh`, `force-merge`, `put-settings`, `reindex`,
   `cluster-health` on older Quickwit builds) and emitting a single
   WARN log per operation type per run.

## Operations matrix

Verified against:

| Component | Version |
|---|---|
| Quickwit (Docker image `quickwit/quickwit:latest`) | 0.8.2-nightly, commit `0f28194d` (built 2024-09-03) |
| Quickwit source reference | local checkout at `b46962585`, tag `qw-azure-20250812` |
| OpenSearch Benchmark | 2.3.0 + this branch |
| Workloads tested | `http_logs` / `append-no-conflicts-index-only` (`--test-mode`) |

Categories: **Native** (passthrough), **Translatable** (mapped to a
different endpoint or shape), **Unsupported** (no-op with WARN log
returning an OS-shape stub), **N/A** (OpenSearch-plugin-specific; not
meaningful for Quickwit). The adapter implements Unsupported and N/A
ops as no-ops with a clear log line so workloads continue to completion.

### Core operations

| Operation | Class | Quickwit route / behavior | Notes |
|---|---|---|---|
| `bulk` | Native | `POST /_elastic/_bulk` and `POST /_elastic/{index}/_bulk` | POST or PUT both accepted by Quickwit. |
| `search` | Native | `GET\|POST /_elastic/{index}/_search` | Also `/_elastic/_search` and `/_elastic/_msearch`. |
| `delete-index` | Native | `DELETE /_elastic/{index}` | |
| `index-stats` | Native | `GET /_elastic/{index}/_stats` | Response does not include `merges.current`. Workloads that gate on `_all.total.merges.current == 0` (e.g. `wait-until-merges-finish`) retry-until-success and eventually error out — cosmetic. |
| `raw-request` | Native | Caller-specified | Pass-through; workload supplies the path. |
| `create-index` | Translatable | `POST /api/v1/indexes` (native) | Adapter best-effort maps the OS mapping to a Quickwit `IndexConfig`. Common field types (`date`, `keyword`, `text`, `integer` family, `ip`, `object`/nested) are explicit; unmapped types fall through to `mode: dynamic`. Multi-fields (`.raw`) and `geo_point` are not translated. Returns OS-shape `{"acknowledged": true}`. |
| `cluster-health` | Unsupported (version-dependent) | no-op + WARN | The `_elastic/_cluster/health` route exists in newer Quickwit source (`b46962585`) but not in the `:latest` Docker image (0.8.2-nightly). Adapter returns a synthetic green response for broad compatibility. |
| `refresh` | Unsupported | no-op + WARN | Quickwit uses commit-based ingestion (see `commit_timeout_secs` in the IndexConfig); no per-request refresh endpoint. Returns `{"_shards":{"total":0,"successful":0,"failed":0}}`. |
| `force-merge` | Unsupported | no-op + WARN | Quickwit manages segment merges internally. Returns `{"_shards":{"total":0,"successful":0,"failed":0}}`. |
| `put-settings` | Unsupported | no-op + WARN | No Quickwit equivalent of OS cluster settings. Returns `{"acknowledged": true, "persistent": {}, "transient": {}}`. |
| `reindex` | Unsupported | no-op + WARN | No `_reindex` endpoint. Returns OS-shape reindex stub with all counts zero. |
| `index` (single-doc) | Unsupported | no-op + WARN | Quickwit has no single-document index endpoint; use `bulk`. |

### Operations marked N/A (OpenSearch-plugin-specific)

These operation types appear in workloads but are not meaningful for
Quickwit. The adapter currently passes them through to the inner
OpenSearch wrapper, which will receive a 404/400 from Quickwit. They
should not appear in workloads run against Quickwit; if they do, future
work is to make each an explicit no-op.

| Operation family | Examples | Notes |
|---|---|---|
| Search/ingest pipelines | `create-search-pipeline`, `put-pipeline`, `delete-pipeline` | OS feature; no Quickwit concept. |
| Snapshots | `create-snapshot`, `restore-snapshot`, `delete-snapshot`, `wait-for-snapshot-create`, `create-snapshot-repository` | Quickwit uses object storage natively; no `_snapshot` API. |
| Transforms | `create-transform`, `start-transform`, `delete-transform`, `wait-for-transform` | OS feature; no Quickwit concept. |
| ML plugin | `create-ml-connector`, `delete-ml-connector`, `register-ml-model`, `register-remote-ml-model`, `delete-ml-model` | OpenSearch ML plugin; no Quickwit equivalent. |
| k-NN plugin | `train-knn-model`, `delete-knn-model`, `warmup-knn-indices` | OpenSearch k-NN plugin. |
| Vector | `vector-search`, `bulk-vector-data-set`, `proto-vector-search` | Quickwit's vector support is not at parity with OpenSearch + JVector. Vector workloads are out of scope. |
| gRPC variants | `proto-bulk`, `proto-search` | gRPC variants of bulk/search. |
| Streaming | `produce-stream-message` | Kafka data producer. |

## Schema translation (create-index)

When an OSB runner calls `client.indices.create(index, body)`, the
adapter translates the OS-style mapping to a Quickwit `IndexConfig`
and POSTs it to `/api/v1/indexes`.

Field type translation:

| OpenSearch field type | Quickwit field type | Notes |
|---|---|---|
| `date` | `datetime` | `input_formats: [rfc3339, unix_timestamp]`, `output_format: unix_timestamp_secs`, `fast: true`. First date field becomes the index `timestamp_field`. |
| `keyword` | `text` with `tokenizer: raw` | OS `"index": false` is honored — `indexed: false` is set and the tokenizer is omitted (Quickwit rejects `tokenizer`/`record`/`fieldnorms` when `indexed: false`). |
| `text` | `text` with `tokenizer: default`, `record: position` | Multi-field syntax (`"fields": {"raw": {...}}`) is NOT translated; queries against `<field>.raw` will fail. |
| `integer`, `long`, `short`, `byte` | `i64`, `fast: true` | |
| `unsigned_long` | `u64`, `fast: true` | |
| `float`, `double`, `half_float`, `scaled_float` | `f64`, `fast: true` | |
| `boolean` | `bool` | |
| `ip` | `ip`, `fast: true` | |
| `object` (or any def with `properties`) | `object` with recursive `field_mappings` | |
| `geo_point`, `geo_shape`, `completion`, others | (not translated) | Falls through to `mode: dynamic`. |

The translated IndexConfig always sets `doc_mapping.mode: dynamic`
(rather than the stricter `strict` mode OS uses) so bulk ingestion is
not rejected by fields the translator can't represent.
`indexing_settings.commit_timeout_secs: 30` is applied.

If the index already exists (Quickwit responds 400 with
"already exist(s)"), the adapter treats this as success — useful when
the user pre-created indices or when delete-index didn't run.

## Running a smoke test

This is the exact sequence used to verify the adapter end-to-end.

### Prerequisites

- Docker (for Quickwit)
- A Python venv with this branch installed in editable mode:
  ```
  python -m venv ~/.venvs/osb-quickwit
  ~/.venvs/osb-quickwit/bin/pip install -e .
  ```
- A local checkout of `opensearch-benchmark-workloads` (default
  location assumed below: `../opensearch-benchmark-workloads`
  relative to this repo).

### Start Quickwit

```sh
docker run -d --rm --name quickwit-bench \
  -p 7280:7280 -p 7281:7281 \
  quickwit/quickwit:latest \
  run

# wait for ready
until curl -sf http://localhost:7280/health/livez | grep -q true; do sleep 1; done
```

### Run the workload

```sh
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES \
~/.venvs/osb-quickwit/bin/osb run \
  --database-type=quickwit \
  --pipeline=benchmark-only \
  --workload-path=../opensearch-benchmark-workloads/http_logs \
  --test-procedure=append-no-conflicts-index-only \
  --target-hosts=localhost:7280 \
  --telemetry="" \
  --test-mode
```

Expected outcome (`--test-mode`):

- 7 indices auto-created in Quickwit (`logs-181998` ... `logs-241998`).
- 7,000 documents ingested (1,000 per chunk).
- Bulk throughput on the order of 50k docs/s on a single-node local
  Quickwit (M-series Mac).
- One WARN log per Unsupported op type:
  `cluster-health`, `create-index`, `refresh`, `force-merge`.
- Cosmetic 100% error rate on `wait-until-merges-finish` (it expects
  `_stats` to include `merges.current`, which Quickwit doesn't).
- Overall run: `SUCCESS`.

### Verify data landed

Quickwit commits asynchronously per `commit_timeout_secs` (30s in our
translated config). Wait, then count:

```sh
sleep 35
for idx in logs-181998 logs-191998 logs-201998 logs-211998 logs-221998 logs-231998 logs-241998; do
  curl -s "http://localhost:7280/api/v1/_elastic/$idx/_stats" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print('$idx:', d['_all']['primaries']['docs']['count'])"
done
```

Sample search:

```sh
curl -s -XPOST "http://localhost:7280/api/v1/_elastic/logs-181998/_search" \
  -H 'content-type: application/json' \
  -d '{"size":1, "query":{"match_all":{}}}' \
  | python3 -m json.tool
```

### Cleanup

```sh
docker stop quickwit-bench
```

## Operational notes

### macOS fork-safety

On macOS with Python 3.13, OSB's Thespian-based actor system forks
worker processes. Several stdlib code paths (notably `urllib.request`)
call into CoreFoundation/SystemConfiguration for proxy detection,
which is unsafe after fork and either crashes the child or
deadlocks it at 100% CPU.

Two mitigations are in place:

1. **Set `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES`** in the
   environment. Required for any OSB run on macOS Python 3.13,
   regardless of the database backend.
2. **The adapter avoids `urllib.request`** internally; the liveness
   probe and the native create-index POST use `http.client` instead
   (no CFNetwork hookup).

### Quickwit commit cadence

Quickwit ingestion is commit-based. The adapter's translated
IndexConfig sets `commit_timeout_secs: 30`. Bulk requests return
`200 {"took": 0, "errors": false}` immediately, but documents are not
queryable until the next commit lands. Benchmark numbers measure
ingest-API latency, not end-to-end indexing latency.

### `cluster-health` version drift

The `_elastic/_cluster/health` route is present in newer Quickwit
source (`b46962585`, tag `qw-azure-20250812`) but absent in the
current `:latest` Docker image (0.8.2-nightly, 2024-09-03). The
adapter assumes the older behavior and always no-ops with synthetic
green to keep workloads portable across versions.

### `wait-until-merges-finish` cosmetic error

This OSB task checks `_all.total.merges.current == 0` in the
`index-stats` response. Quickwit's stats response doesn't include
`merges` (it has no OpenSearch-style merge concept). The task is
`retry-until-success`, so it loops until OSB gives up and records a
100% error rate. The overall run still succeeds. Future work could
extend the adapter to inject a synthetic `merges.current: 0` into the
stats response.

## Files in this directory

| File | Purpose |
|---|---|
| `__init__.py` | Package marker. |
| `quickwit.py` | `QuickwitClientFactory`, `QuickwitDatabaseClient`, namespace wrappers, OS→Quickwit schema translator, fork-safe HTTP helpers. |
| `README.md` | This document. |

Reference example IndexConfig (manual pre-creation path, mostly
informational now that the adapter auto-translates):
`examples/quickwit-indexes/http_logs.yaml` at the repo root.

## Known limitations and follow-ups

- N/A bucket ops are not yet explicit no-ops; if a workload invokes
  one, it hits Quickwit and gets a 404/400. Add explicit no-op
  overrides for any op family used by a workload of interest.
- Vector workloads are out of scope. Quickwit's vector support is not
  yet at parity with the `opensearch-jvector` plugin.
- Only `http_logs` / `append-no-conflicts-index-only` has been
  exercised end-to-end. Other log-shaped workloads (`nyc_taxis`,
  `geonames`) likely work but are unverified.
- Multi-field syntax (`"request": {..., "fields": {"raw": {...}}}`)
  in OS mappings is not translated. Queries against `<field>.raw`
  will fail. Workaround: name the sub-field as a separate top-level
  field in the OS mapping.
