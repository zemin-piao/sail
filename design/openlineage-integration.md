# OpenLineage Integration for Sail

Status: **exploration / design note**. Nothing here is implemented.
Scope: what it would take for Sail to emit [OpenLineage](https://openlineage.io) run events,
which parts of the existing architecture already carry the information we need,
and where the genuinely hard problems are.

All file references are to `main` at the time of writing.

---

## 1. Summary

Sail is unusually well positioned for OpenLineage:

- **A single execution funnel.** Every Spark Connect execution passes through
  `handle_execute_plan` (`crates/sail-spark-connect/src/service/plan_executor.rs:119`),
  which itself calls `resolve_and_execute_plan` (`crates/sail-plan/src/lib.rs:34`).
  Spark needs a `SparkListener` and a large amount of reflection to reach the equivalent point;
  Sail has one function.
- **Reads and writes are already reified as typed plan nodes** that carry the physical
  location. `TableScan` keeps its `TableReference`, and each table format's `TableSource`
  exposes a URI (`DeltaTableSource::log_store()`, `IcebergTableProvider::table_uri()`,
  `ListingTableSource::config().table_paths`). Writes are per-format logical extension nodes
  (`FileWriteNode`, `DeltaWriteNode`, `IcebergWriteNode`) that hold the target URL directly.
- **There is a precedent for plan-walking cross-cutting concerns.** `sail-telemetry`
  already rewrites the physical plan to attach spans and metrics
  (`crates/sail-telemetry/src/execution/physical_plan.rs:54`), and the config system
  (`crates/sail-common/src/config/application.yaml`) already models an exporter-style subsystem.

The recommendation is a new `sail-lineage` crate plus a `LineageService` session extension,
with explicit lifecycle calls in the Spark Connect and Flight SQL frontends. Table-level
lineage is straightforward. Column-level lineage is feasible but blocked on one specific
internal detail described in §5.

---

## 2. What OpenLineage actually requires

A `RunEvent` is a JSON document with:

| Field | Meaning |
| --- | --- |
| `eventType` | `START`, `RUNNING`, `COMPLETE`, `ABORT`, `FAIL`, `OTHER` |
| `eventTime` | ISO-8601 timestamp |
| `run` | `{ runId: UUID, facets }` |
| `job` | `{ namespace, name, facets }` |
| `inputs[]` | datasets `{ namespace, name, facets }` |
| `outputs[]` | datasets `{ namespace, name, facets }` |
| `producer`, `schemaURL` | provenance of the emitter |

At least one `START` and one terminal event (`COMPLETE`/`FAIL`/`ABORT`) per run is required.
Everything beyond the core model is a *facet* — an extensible, individually versioned
metadata blob. Custom facets must be prefixed with the producing project's name
(so: `sail_*`).

Dataset naming is specified, and Sail's URL handling maps onto it cleanly:

| Storage | Namespace | Name |
| --- | --- | --- |
| S3 | `s3://{bucket}` | `{object key}` (no leading slash; `s3a`/`s3n` normalize to `s3`) |
| GCS | `gs://{bucket}` | `{object key}` |
| ADLS Gen2 | `abfss://{container}@{account}.dfs.core.windows.net` | `{path}` |
| HDFS | `hdfs://{host}:{port}` | `{path}` |
| Local FS | `file://{host}` | `{path}` |

Job naming is a convention rather than a rule: namespace comes from client configuration,
name must be unique within it. Spark's integration uses `{appName}.{command}.{table}`.

---

## 3. Mapping Sail's execution lifecycle onto run events

### 3.1 The funnel

```
Spark Connect / Flight SQL request
  └─ handle_execute_plan                  plan_executor.rs:119
       ├─ resolve_and_execute_plan        sail-plan/src/lib.rs:34
       │    ├─ PlanResolver::resolve_named_plan   → NamedPlan { plan, fields }
       │    ├─ session_state.optimize(&plan)      → final LogicalPlan   ◄── lineage source
       │    └─ create_physical_plan               → ExecutionPlan
       ├─ JobService::runner().execute(ctx, plan) → SendableRecordBatchStream
       └─ ExecutePlanMode::Lazy    → Executor::start / Executor::run   ◄── terminal event
          ExecutePlanMode::EagerSilent → read_stream(stream).await     ◄── terminal event
```

`handle_execute_plan` covers relation execution, `WriteOperation`, `WriteOperationV2`,
`MergeIntoTableCommand`, view creation and UDF registration — that is, every DataFrame and
SQL action. `handle_execute_sql_command` and `handle_execute_write_stream_operation_start`
call `resolve_and_execute_plan` directly and need their own hooks. Flight SQL has a parallel
call site at `crates/sail-flight/src/service.rs:107`.

### 3.2 Event mapping

| Sail moment | OpenLineage |
| --- | --- |
| Spark Connect session created (`SparkSession::try_new`) | parent `START` for an application-level run |
| `resolve_and_execute_plan` returns; before `runner().execute` | child `START` with full `inputs`/`outputs` |
| Streaming query micro-batch (future) | `RUNNING` |
| `Executor::run` completes, or `read_stream` returns `Ok` | `COMPLETE` + `outputStatistics` facet |
| Any error on the path | `FAIL` + `errorMessage` facet |
| Session expiry / `interrupt` | `ABORT` |

The parent/child split matters: OpenLineage's `parent` run facet is how consumers group a
session's queries. Sail's `SparkSession` already owns a stable `session_id` and `user_id`
(`crates/sail-spark-connect/src/session.rs:29`), so the parent run is essentially free.

**Proposed identity scheme:**

- Parent job: namespace `{configured}`, name `{app_name or session_id}`; run ID =
  UUIDv5 of the session ID, so the same session always yields the same run ID.
- Child job: name `{parent}.{operation_kind}[.{primary_output}]`, e.g.
  `session-abc.write.default.events`. Run ID = UUIDv5 of `(session_id, operation_id)`,
  where `operation_id` already exists on `ExecutorMetadata`
  (`crates/sail-spark-connect/src/executor.rs:94`).

Using v5 rather than v4 makes events idempotent under retry and reattach — Spark Connect
explicitly supports reattachable execution, so retries are not hypothetical.

---

## 4. Identifying datasets

### 4.1 Reads

After resolution the plan still contains `LogicalPlan::TableScan` with its original
`TableReference` (`crates/sail-plan/src/resolver/query/read.rs:548`). The scan is wrapped in
a rename projection, but the node itself is intact. For each scan we need
`(namespace, name)` and a schema facet.

The `TableReference` alone is not enough — OpenLineage wants physical identity, with the
catalog name carried separately in a `symlinks` facet. The URI must come from the
`TableSource`, which means a downcast per format:

| Format | Source type | URI accessor |
| --- | --- | --- |
| Listing (Parquet/CSV/JSON/…) | `ListingTableSource` | `.config().table_paths: Vec<ListingTableUrl>` |
| Delta | `DeltaTableSource` | `.log_store().root_uri()` (`crates/sail-delta-lake/src/delta_log/store.rs:162`) |
| Iceberg | `IcebergTableSource` | `.provider().table_uri()` (`crates/sail-iceberg/src/datasource/provider.rs:220`) |
| System tables | `SystemTableSource` | synthetic namespace, e.g. `sail://system` |

A chain of `downcast_ref` calls in `sail-lineage` would work but inverts the dependency
graph: the lineage crate would have to depend on `sail-delta-lake`, `sail-iceberg`,
`sail-data-source` and every format added later. That is the wrong shape for a codebase that
deliberately routes formats through the `TableFormat` trait
(`crates/sail-common-datafusion/src/datasource.rs:479`).

**Preferred alternative — a trait in `sail-common-datafusion`:**

```rust
// Sketch — not compiled.
pub struct DatasetIdentity {
    pub namespace: String,
    pub name: String,
    /// Catalog-qualified aliases for the OpenLineage `symlinks` facet.
    pub symlinks: Vec<(String, String)>,
    /// Table format version, if the format is versioned (Delta/Iceberg).
    pub version: Option<String>,
}

/// Implemented by `TableSource`s and write nodes that correspond to a
/// physical dataset. Returning `None` means "not lineage-visible".
pub trait LineageDataset {
    fn dataset_identity(&self) -> Option<DatasetIdentity>;
}
```

Each format crate implements this next to its existing `TableSource`/write node. The
extractor then only needs one downcast, against a trait it owns. Formats that opt out
(`rate`, `socket`, `console`, `noop`) simply do not implement it, and new formats are
reminded to consider lineage by the trait's presence.

### 4.2 Writes

Writes resolve to `LogicalPlan::Extension(BarrierNode { preconditions, input })`
(`crates/sail-plan/src/resolver/command/write.rs:533`), where the inner node is
format-specific:

- `FileWriteNode` (`crates/sail-data-source/src/listing/write.rs:27`) — holds
  `FileWriteOptions { url, overwrite, partition_by, sort_by, format }`.
- `DeltaWriteNode` (`crates/sail-delta-lake/src/table_format.rs:546`) — holds `path`, `mode`,
  `partition_by`, `lakehouse_table`.
- `IcebergWriteNode` (`crates/sail-iceberg/src/table_format.rs:310`).

All three carry exactly what the output dataset needs. The same `LineageDataset` trait
covers them. Two details worth getting right:

- **`BarrierNode.preconditions`** can contain a `CREATE TABLE` catalog command. That is a
  lineage-relevant fact (the run created the dataset) and should set
  `lifecycleStateChange: CREATE` on the output facet rather than being skipped.
- **`SinkMode`** maps directly onto the `lifecycleStateChange` facet:
  `Overwrite` → `OVERWRITE`, `Append` → `APPEND`, `ErrorIfExists` → `CREATE`,
  `TruncateIf` (Delta `replaceWhere`) → `OVERWRITE` with the predicate in a
  `sail_replaceWhere` custom facet.

### 4.3 Row-level operations

MERGE / UPDATE / DELETE go through `sail-plan/src/resolver/command/{merge,delete}.rs` and
produce plans containing both a read and a write of the same table, plus the internal
`__sail_operation_type` column (`crates/sail-common-datafusion/src/datasource.rs:34`).
The target table must appear in **both** `inputs` and `outputs`. The internal metadata
columns (`__sail_file_path`, `__sail_file_row_index`, `__sail_operation_type`,
`__sail_merge_source_metric`) must be filtered out of every schema and column-lineage facet —
they are Sail implementation details and would leak into user-facing catalogs otherwise.

---

## 5. Column-level lineage: the blocker

This is where the design gets interesting, and where a naive port of the Spark integration
would silently produce garbage.

`PlanResolver` renames **every** field to an opaque internal ID of the form `#0`, `#1`, `#2`
(`crates/sail-plan/src/resolver/state.rs:120`) to avoid name collisions during resolution.
The user-facing names live in `PlanResolverState.fields: HashMap<String, FieldInfo>` and are
reapplied only at the very end:

- for a query's *output* columns, via `NamedPlan.fields`
  (`crates/sail-plan/src/resolver/plan.rs:9`), consumed by `rename_physical_plan`;
- for everything else — never. The optimized logical plan that a lineage extractor would walk
  is entirely `#N`.

So a column-lineage extractor running over the final logical plan can correctly compute the
*graph* (`#7` derives from `#3` and `#4`), and can name the leaves (a `TableScan`'s schema
still has real column names before the rename projection) and the roots
(`NamedPlan.fields`), but it cannot name intermediate columns without the mapping.

**Two ways out:**

1. **Extract before optimization.** `PlanResolver::resolve_named_plan` returns while the
   state is still alive; the extractor could run there. Downside: lineage would reflect the
   user's plan, not the executed one — projection pushdown, subquery decorrelation and
   constant folding all change which columns are genuinely read. Spark's integration uses
   the optimized plan for exactly this reason.
2. **Export the field-name map alongside the plan** (recommended). Widen `NamedPlan` to
   carry the full `#N → user-facing name` mapping, and thread it out of
   `resolve_and_execute_plan`:

   ```rust
   // Sketch — not compiled.
   pub struct NamedPlan {
       pub plan: LogicalPlan,
       pub fields: Option<Vec<String>>,
       /// `#N` → user-facing name, for consumers that need to interpret
       /// internal column identifiers (lineage, EXPLAIN, diagnostics).
       pub field_names: Arc<HashMap<String, String>>,
   }
   ```

   `FieldInfo` (`state.rs:14`) already stores `name`, `plan_ids` and `hidden`, so the map is
   a projection of data that exists. `FieldInfo::is_hidden()` conveniently marks the
   synthetic join/sort helper columns that should be excluded from lineage.

   This also changes `resolve_and_execute_plan`'s signature, which currently discards the
   logical plan entirely and returns only `(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)`.
   Six call sites are affected (two in `sail-spark-connect`, one in `sail-flight`, one in
   `proto/function.rs`). Returning a small struct instead of a tuple would be a readability
   improvement independent of lineage.

**Expression-level extraction** itself is mechanical: walk each `Projection`/`Aggregate`/
`Window` and, for every output `Expr`, collect `Expr::Column` references via
`Expr::column_refs()`, distinguishing `DIRECT` transformations (a bare column or a cast)
from `INDIRECT` ones (filter/join/group-by predicates), as the `columnLineage` facet
requires.

---

## 6. Facets worth emitting

**Standard:**

| Facet | Source in Sail |
| --- | --- |
| `schema` (dataset) | `TableSource::schema()` / write node input schema |
| `dataSource` (dataset) | the URI + scheme derived in §4.1 |
| `symlinks` (dataset) | `TableScan.table_name` and `LakehouseExecutionContext::catalog_table()` |
| `columnLineage` (output dataset) | §5 |
| `lifecycleStateChange` (output dataset) | `SinkMode` |
| `version` (dataset) | `DeltaSnapshot::version()`; Iceberg snapshot ID |
| `outputStatistics` (output dataset) | physical plan `MetricsSet` after execution |
| `parent` (run) | session run ID from `SparkSession::session_id()` |
| `errorMessage` (run) | `SparkError` / `PlanError` on the failure path |
| `processing_engine` (run) | `sail`, `env!("CARGO_PKG_VERSION")`, `openlineage_adapter_version` |
| `sql` (job) | original SQL text, available for `handle_execute_sql_command` |

**Sail-specific (prefixed `sail_`):**

- `sail_executionMode` — `local` / `local-cluster` / `kubernetes-cluster` from `AppConfig::mode`.
- `sail_physicalPlan` — the `FinalPhysicalPlan` string that `resolve_and_execute_plan`
  already builds (`sail-plan/src/lib.rs:61`). Should be opt-in; plan strings are large and
  can contain literal values from the query.
- `sail_jobMetrics` — stage/partition counts from `JobService`, which already tracks jobs,
  stages and workers for the system tables (`crates/sail-common-datafusion/src/session/job.rs:260`).

`outputStatistics` deserves a note: it needs row/byte counts from the *sink*, after
execution. `sail-telemetry` already wraps every operator in `TracingExec` and harvests
`MetricsSet` (`crates/sail-telemetry/src/execution/physical_plan.rs`), so the plumbing
exists — but today it feeds OTLP metrics, not a per-run aggregate. Reusing it means either
keeping a handle to the root `ExecutionPlan` until completion (cheap, it is an `Arc`) or
adding a per-run metrics collector.

---

## 7. Proposed shape

### 7.1 Crate layout

```
crates/sail-lineage/
  src/
    lib.rs
    config.rs        // LineageConfig ← AppConfig
    service.rs       // LineageService: SessionExtension
    event.rs         // RunEvent model + facets (serde)
    naming.rs        // URL → (namespace, name), per §2
    extract/
      mod.rs         // LogicalPlan → Lineage { inputs, outputs }
      column.rs      // columnLineage facet
    transport/
      mod.rs         // trait Transport
      http.rs        // reqwest (already a workspace dep, Cargo.toml:107)
      file.rs        // newline-delimited JSON, for tests
      console.rs
```

`sail-lineage` depends on `sail-common`, `sail-common-datafusion`, `datafusion` and
`reqwest` — deliberately **not** on the format crates, which is what the `LineageDataset`
trait in §4.1 buys.

Wiring: register `LineageService` in `ServerSessionFactory::create_session_config`
(`crates/sail-session/src/session_factory/server.rs:124`), alongside `JobService` and
`ActivityTracker`.

### 7.2 Emission must never block or fail a query

Non-negotiable. The transport should own a bounded `tokio::sync::mpsc` channel and a
background task; `emit()` does a `try_send` and increments a dropped-event counter on a full
queue. A lineage backend being down, slow or misconfigured must degrade to a log line.
`sail-telemetry`'s `BatchLogProcessor` setup (`crates/sail-telemetry/src/telemetry.rs`) is
the right model.

The workspace lints make this easy to get right by accident: `unwrap_used`, `expect_used`
and `panic` are all `deny` at the workspace level (`Cargo.toml:17-23`).

### 7.3 Configuration

Following the declarative pattern in `crates/sail-common/src/config/application.yaml`
(a YAML definition plus a matching `serde` struct in `application.rs`):

```yaml
- key: lineage.enabled
  type: boolean
  default: "false"
  description: Whether to emit OpenLineage events for query executions.
  experimental: true

- key: lineage.transport
  type: string
  default: "http"
  description: |
    The OpenLineage transport. Valid values are `http`, `file`, and `console`.
  experimental: true

- key: lineage.url
  type: string
  default: ""
  description: The OpenLineage receiver endpoint, e.g. a Marquez server.
  experimental: true

- key: lineage.api_key
  type: string
  default: ""
  description: The bearer token for the OpenLineage receiver.
  experimental: true

- key: lineage.namespace
  type: string
  default: "sail"
  description: The OpenLineage job namespace.
  experimental: true

- key: lineage.column_lineage
  type: boolean
  default: "true"
  description: Whether to compute the column-level lineage facet.
  experimental: true

- key: lineage.queue_size
  type: number
  default: "2048"
  description: |
    The maximum number of pending OpenLineage events.
    Events are dropped when the queue is full so that emission never blocks execution.
  experimental: true
```

All keys are settable as `SAIL_LINEAGE__URL` etc. via the existing `figment` env-var
mapping. `api_key` should use `secrecy::SecretString`, as the catalog credentials already do.

---

## 8. Prior art and the build-vs-depend question

Two Rust crates now exist, both from the `open-lakehouse/headwaters` project, both
Apache-2.0, both first published 2026-06-27:

| Crate | Version | What it is |
| --- | --- | --- |
| `openlineage-client` | 0.0.4 | Transport-agnostic event model + non-blocking client. ~950 LOC, MSRV 1.91. |
| `datafusion-openlineage` | 0.0.7 | Wraps a `SessionState`'s `QueryPlanner`; emits `START` at plan time and `COMPLETE`/`FAIL` at end of execution. Column-level lineage. Depends on `datafusion ^54`. |

`datafusion ^54` matches Sail exactly (`Cargo.toml:153`), so this is worth a spike rather
than a dismissal. The technique — wrapping the query planner — would hook Sail at
`ExtensionQueryPlanner` (`crates/sail-session/src/planner.rs`) with no changes to any call
site, which is attractive.

But it will not be sufficient on its own:

- It sees generic DataFusion nodes. Sail's reads and writes are **custom** `TableSource`
  implementations and `UserDefinedLogicalNode`s, so every Sail dataset would be invisible
  to it without the per-format extraction described in §4 anyway.
- It cannot resolve `#N` column IDs (§5), so its column lineage would be nameless on Sail.
- A `0.0.x` crate from a three-month-old repository, with ~1.2k total downloads and its own
  README recommending Marquez for production, is not a dependency to put on Sail's critical
  path.

**Recommendation:** implement the extraction in `sail-lineage` (Sail-specific, unavoidable),
and evaluate `openlineage-client` for the event model and transport only — that is the
commodity half, it is small enough to vendor or reimplement if it stalls, and it keeps Sail
honest about spec conformance. Decide after a spike; do not block Phase 1 on it.

The `headwaters` server itself is explicitly experimental. **Marquez** remains the reference
receiver to test against, and it runs from a single Docker Compose file — which fits Sail's
existing `compose.yml`-based development setup.

Note also that `openlineage-sql`, OpenLineage's SQL-parsing lineage extractor, is itself
written in Rust — but it is not published to crates.io and it only parses SQL text. Sail
resolves SQL to a typed plan, which is strictly better information, so it is not useful here.

---

## 9. Gaps and open questions

1. **Streaming.** `handle_execute_write_stream_operation_start` starts a long-running query.
   OpenLineage models this as one run with periodic `RUNNING` events, but Sail's streaming
   layer (`crates/sail-plan/src/streaming/`) has no per-micro-batch callback today. Phase 3
   at the earliest; the design should not paint itself into a corner by assuming runs are
   short.
2. **Cluster mode.** In `KubernetesCluster` mode, work is distributed across workers. Events
   should be emitted **only from the driver** — `JobService` already has the driver-side
   view. Worker sessions (`WorkerSessionFactory`) must not register a `LineageService`.
3. **Views and CTEs.** A temporary view (`CreateDataFrameViewCommand`) is not a dataset. The
   run that creates it produces no output dataset; the runs that *use* it should report the
   view's underlying tables as inputs. `CatalogManager::get_tracked_logical_plan`
   (`crates/sail-catalog/src/manager/mod.rs:144`) makes this resolvable, but it needs an
   explicit decision rather than falling out by default.
4. **Python UDFs / `MapPartitionsNode`.** Column lineage cannot see through a Python UDF.
   These must be reported as `INDIRECT` with no transformation description, never silently
   dropped — a missing edge is worse than a vague one.
5. **PII in facets.** `sql` and `sail_physicalPlan` facets embed literal values from user
   queries. Both should be opt-in, and this should be stated in the user documentation.
6. **Test strategy.** Golden-file tests over emitted JSON fit `sail-gold-test`, which the
   repo already uses. Plus a Marquez container in `compose.yml` for an end-to-end check, and
   validation against the published `OpenLineage.json` schema.

---

## 10. Phasing

| Phase | Deliverable | Rough shape |
| --- | --- | --- |
| **0** | Spike: `LineageDataset` trait + extractor over the final logical plan, printing to stdout. No transport, no config. Answers "does the plan actually carry what we need?" | small, throwaway |
| **1** | Table-level lineage. `sail-lineage` crate, `LineageService`, config, HTTP + console transports, `START`/`COMPLETE`/`FAIL` from `handle_execute_plan`, `schema`/`dataSource`/`symlinks`/`lifecycleStateChange` facets, Marquez end-to-end test. | the real milestone |
| **2** | Column-level lineage. Requires the `NamedPlan` change in §5. `columnLineage` facet, `outputStatistics` from plan metrics, parent/child run correlation. | depends on Phase 1 |
| **3** | Streaming runs, cluster-mode job facets, Flight SQL frontend, user documentation under `docs/guide/integrations/`. | long tail |

Phase 1 is the point at which Sail becomes usable with Marquez, DataHub, Atlan, Astronomer
and every other OpenLineage consumer. Phase 2 is what makes it competitive with Spark's
integration rather than merely equivalent.

---

## 11. Smallest honest next step

Phase 0, as one throwaway binary:

1. Add `dataset_identity()` to `ListingTableSource`, `DeltaTableSource`, `IcebergTableSource`,
   `FileWriteNode` and `DeltaWriteNode`.
2. Write a `LogicalPlan` visitor that collects inputs and outputs and prints them.
3. Call it from `resolve_and_execute_plan` behind `SAIL_LINEAGE__ENABLED`.
4. Run the existing Spark test suite against it and diff what it finds versus what the
   queries actually touch.

That measures the real coverage gap — including how many plans produce datasets the
extractor cannot name — before committing to a crate, a config surface or a transport.

---

## References

- [OpenLineage spec (`OpenLineage.json`)](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json)
- [OpenLineage naming conventions](https://openlineage.io/docs/spec/naming/)
- [OpenLineage custom facets](https://openlineage.io/docs/spec/facets/custom-facets/)
- [`openlineage-client` crate](https://crates.io/crates/openlineage-client)
- [`datafusion-openlineage` crate](https://crates.io/crates/datafusion-openlineage)
- [`open-lakehouse/headwaters`](https://github.com/open-lakehouse/headwaters)
- [OpenLineage SQL integration (Rust)](https://github.com/OpenLineage/OpenLineage/tree/main/integration/sql)
