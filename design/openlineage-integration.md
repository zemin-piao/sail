# OpenLineage Integration for Sail

Status: **exploration / design note**. Nothing here is implemented.
Scope: what it would take for Sail to emit [OpenLineage](https://openlineage.io) run events,
which parts of the existing architecture already carry the information we need,
and where the genuinely hard problems are.

All file references are to `main` at the time of writing. Claims marked *(verified)* were
checked against the source; everything else is a proposal.

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
- **Those nodes survive optimization** *(verified)*. `sail_logical_optimizer::default_optimizer_rules()`
  (`crates/sail-logical-optimizer/src/lib.rs:24`) is DataFusion's stock rule set plus
  `DecorrelateLateralProjection` and `ResolveLambdaVariables`. Nothing rewrites scans or write
  extension nodes at the logical level, so an extractor over the optimized plan still sees them.
- **There is an established pattern for per-format plan handling.** `ExtensionQueryPlanner`
  composes a `Vec<Arc<dyn ExtensionPlanner>>`, one contributed by each format crate
  (`crates/sail-session/src/planner.rs:72`), and each does its own concrete downcast in its
  own crate (`crates/sail-data-source/src/listing/planner.rs:85`). Lineage extraction should
  mirror this exactly — see §5.1.

The recommendation is a new `sail-lineage` crate plus a `LineageService` session extension,
with explicit lifecycle calls in the Spark Connect and Flight SQL frontends. Table-level
lineage is straightforward. Two things are not: **event volume** (§4) is the constraint most
likely to make the feature unusable in practice, and **column-level lineage** (§6) is blocked
on an internal naming detail.

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
name must be unique within it. The spec's own framing is load-bearing and easy to miss:
**"A `Job` is a recurring data transformation."** A job is not an execution — it is the thing
that executes repeatedly, and each execution is a *run*. §4 is entirely about getting this
distinction right.

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

**Run IDs.** Derive them as UUIDv5:

- Parent run ID = v5 of the session ID.
- Child run ID = v5 of `(session_id, operation_id)`, where `operation_id` already exists on
  `ExecutorMetadata` (`crates/sail-spark-connect/src/executor.rs:94`).

This makes **reattach** idempotent — Spark Connect's reattachable execution resumes the same
operation ID, so a re-emitted `START` carries the same run ID and consumers deduplicate it.
It does **not** make client-side *retries* idempotent: a retried query gets a fresh operation
ID and therefore a fresh run, which is arguably correct anyway (it is a second execution).
The one case to handle explicitly is a duplicate `START` for a run that already reached a
terminal state — after a server restart, say. Consumers vary in how they treat this; the
emitter should track terminal runs in the `LineageService` and suppress the re-`START`.

Job *names* are a separate and much harder problem — see next section.

---

## 4. Event volume and job identity

This is the constraint that decides whether the feature is usable, and it is easy to get
wrong in a way that only shows up after a week in production.

### 4.1 The problem

Sail is a drop-in Spark replacement sold on interactive speed. A single exploratory session
issues dozens of `.show()`, `.count()`, `.describe()` and intermediate DataFrame actions.
Every one of them reaches `handle_execute_plan`. Naively emitting a run event per execution
means:

- a flood of events for queries that produce no dataset and teach a catalog nothing;
- worse, if the **job name** embeds anything session-scoped, every session mints a fresh set
  of job identities that persist in the catalog forever. After a month the catalog is mostly
  single-run jobs from someone's laptop.

This is a known failure mode of Spark's own OpenLineage integration, and Sail's speed makes
it strictly worse: more queries per unit time.

### 4.2 Job identity must be session-independent

The fix follows from the spec's definition (§2): a job is the recurring transformation, a run
is one execution of it. So:

- **Job name must not contain the session ID, operation ID, or a timestamp.** Two runs of
  the same nightly write must land on the *same* job so a consumer can show its run history.
- **Job name should be derived from the output dataset**, which is the stable thing:
  `{app_name}.{operation}.{output_namespace}/{output_name}`, e.g.
  `etl-nightly.write.s3://warehouse/events`. Falling back to `sail` when no app name is set.
- The session ID belongs in the **run** (via the `parent` facet), never the job.

An earlier draft of this note proposed `{parent}.{operation_kind}.{primary_output}` with
`parent` being the session — that is precisely the bug described above.

### 4.3 Read-only queries have no stable identity

A `.show()` has no output dataset, so there is nothing stable to name a job after. Options,
none of them clean:

1. **Do not emit** (recommended default). Runs with no output dataset are dropped. The
   catalog then holds only lineage-bearing events, which is what dataset-lineage consumers
   actually consume.
2. **Emit under a plan hash**: job name `{app_name}.query.{hash of normalized plan}`. Repeated
   identical queries collapse onto one job. Useful for query-observability use cases, useless
   for ad-hoc exploration where no two plans match.
3. **Emit everything** under a per-session job. Honest, and immediately pollutes the catalog.

Proposal: a `lineage.emit` setting with values `writes` (default), `all`, and expose (2) as
`queries` for the observability case. Shipping with `writes` as the default is the single
most important decision in this design — a user who wants everything can opt in, but nobody
gets a wrecked catalog by turning the feature on.

### 4.4 Cost when disabled and when enabled

- **Disabled**: the extractor must not run and must allocate nothing. This rules out
  unconditionally attaching lineage state to core types (see §6).
- **Enabled**: extraction walks the logical plan once. That is cheap relative to planning, but
  it is on the query path, so the column-lineage pass in particular should be behind its own
  flag (`lineage.column_lineage`) and skipped for runs that will be filtered out by §4.3
  anyway — decide emit-or-not *before* doing the expensive extraction.

---

## 5. Identifying datasets

### 5.1 How to reach format-specific information

After resolution the plan still contains `LogicalPlan::TableScan` with its original
`TableReference` (`crates/sail-plan/src/resolver/query/read.rs:548`). The scan is wrapped in
a rename projection, but the node itself is intact. The `TableReference` alone is not enough —
OpenLineage wants physical identity, with the catalog name carried separately in a `symlinks`
facet — so the URI must come from the `TableSource`:

| Format | Source type | URI accessor |
| --- | --- | --- |
| Listing (Parquet/CSV/JSON/…) | `ListingTableSource` | `.config().table_paths: Vec<ListingTableUrl>` |
| Delta | `DeltaTableSource` | `.log_store().root_uri()` (`crates/sail-delta-lake/src/delta_log/store.rs:162`) |
| Iceberg | `IcebergTableSource` | `.provider().table_uri()` (`crates/sail-iceberg/src/datasource/provider.rs:220`) |
| System tables | `SystemTableSource` | synthetic namespace, e.g. `sail://system` |

**A shared trait does not work here.** The obvious design — a `LineageDataset` trait in
`sail-common-datafusion` that each format implements — cannot be reached from a plan. A
`TableScan` holds an `Arc<dyn TableSource>`, and `TableSource::as_any()` yields `&dyn Any`,
which downcasts only to a **concrete** type. Rust has no cross-casting from one trait object
to another, so there is no path from `dyn TableSource` to `dyn LineageDataset` without
already knowing the concrete type — which is the coupling the trait was meant to remove.
`UserDefinedLogicalNode` has the same shape and the same problem.

**Use the pattern the codebase already uses for exactly this.** Physical planning solves the
identical problem with a list of per-format planners, each doing its own concrete downcast
inside its own crate:

```rust
// crates/sail-session/src/planner.rs:72 — existing code
let extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
    Arc::new(DeltaPhysicalPlanner),
    Arc::new(IcebergPhysicalPlanner),
    Arc::new(ListingPhysicalPlanner),
    // ...
];
```

Lineage extraction mirrors it:

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

/// Contributed by each format crate. Returns `None` for nodes it does not own.
pub trait LineageExtractor: Send + Sync {
    fn read_dataset(&self, source: &dyn TableSource) -> Option<DatasetIdentity>;
    fn write_dataset(&self, node: &dyn UserDefinedLogicalNode) -> Option<DatasetIdentity>;
}
```

`DeltaLineageExtractor` lives in `sail-delta-lake` and does
`source.as_any().downcast_ref::<DeltaTableSource>()`, exactly as `DeltaPhysicalPlanner` already
does. `sail-session` composes the list — the same crate that already composes
`extension_planners` and `create_table_format_registry()`
(`crates/sail-session/src/formats.rs:20`). `sail-lineage` depends on none of the format crates.

Honest cost of this shape: it is **runtime registration, not a compile-time contract**. Add a
new format and forget to add its extractor and its datasets are silently invisible. An opt-in
trait would have had the same gap; only a required method on `TableFormat` would force the
issue, at the cost of breaking every format impl. Mitigation is a test that asserts every
registered `TableFormat` name has a corresponding extractor, with an explicit opt-out list for
the formats that genuinely have no dataset (`rate`, `socket`, `console`, `noop`).

### 5.2 Naming is not as clean as the table suggests

Two unsolved cases, both real:

- **Multi-path and glob listing sources.** `ListingTableSource` holds `Vec<ListingTableUrl>`,
  and a `ListingTableUrl` can be a glob. What is the dataset name for
  `s3://bucket/data/year=*/`? OpenLineage has no good answer. Candidates: the longest
  non-glob prefix (loses precision, but stable and joins correctly with a write to the same
  root), or one dataset per resolved path (explodes cardinality). Prefix is probably right;
  it needs a decision, and the glob pattern should go in a `sail_pathPattern` custom facet so
  the information is not lost.
- **URI normalization.** The same physical dataset reached as `s3a://b/p`, `s3://b/p` and
  `s3://b/p/` must produce one identity, or the lineage graph silently forks. `naming.rs`
  needs a canonicalization pass (scheme aliasing, trailing-slash stripping, no leading slash
  on the name) applied to *every* URI before it becomes a dataset name, with unit tests. This
  is small but it is the difference between a connected graph and confetti.

### 5.3 Writes

Writes resolve to `LogicalPlan::Extension(BarrierNode { preconditions, input })`
(`crates/sail-plan/src/resolver/command/write.rs:533`), where the inner node is
format-specific:

- `FileWriteNode` (`crates/sail-data-source/src/listing/write.rs:27`) — holds
  `FileWriteOptions { url, overwrite, partition_by, sort_by, format }`.
- `DeltaWriteNode` (`crates/sail-delta-lake/src/table_format.rs:546`) — holds `path`, `mode`,
  `partition_by`, `lakehouse_table`.
- `IcebergWriteNode` (`crates/sail-iceberg/src/table_format.rs:310`).

All three carry what the output dataset needs; `LineageExtractor::write_dataset` covers them.
Two details worth getting right:

- **`BarrierNode.preconditions`** can contain a `CREATE TABLE` catalog command. That is a
  lineage-relevant fact (the run created the dataset) and should set
  `lifecycleStateChange: CREATE` on the output facet rather than being skipped.
- **`SinkMode`** maps directly onto the `lifecycleStateChange` facet:
  `Overwrite` → `OVERWRITE`, `Append` → `APPEND`, `ErrorIfExists` → `CREATE`,
  `TruncateIf` (Delta `replaceWhere`) → `OVERWRITE` with the predicate in a
  `sail_replaceWhere` custom facet.

### 5.4 Row-level operations

MERGE / UPDATE / DELETE go through `sail-plan/src/resolver/command/{merge,delete}.rs` and
produce plans containing both a read and a write of the same table, plus the internal
`__sail_operation_type` column (`crates/sail-common-datafusion/src/datasource.rs:34`).
The target table must appear in **both** `inputs` and `outputs`. The internal metadata
columns (`__sail_file_path`, `__sail_file_row_index`, `__sail_operation_type`,
`__sail_merge_source_metric`) must be filtered out of every schema and column-lineage facet —
they are Sail implementation details and would leak into user-facing catalogs otherwise.

---

## 6. Column-level lineage: the blocker

`PlanResolver` renames **every** field to an opaque internal ID of the form `#0`, `#1`, `#2`
(`crates/sail-plan/src/resolver/state.rs:120`) to avoid name collisions during resolution.
The user-facing names live in `PlanResolverState.fields: HashMap<String, FieldInfo>` and are
reapplied only at the very end:

- for a query's *output* columns, via `NamedPlan.fields`
  (`crates/sail-plan/src/resolver/plan.rs:9`), consumed by `rename_physical_plan`;
- for everything else — never. The optimized logical plan that a lineage extractor would walk
  is entirely `#N`.

So an extractor over the final logical plan can correctly compute the *graph* (`#7` derives
from `#3` and `#4`), and can name the leaves (a `TableScan`'s schema still has real column
names before the rename projection) and the roots (`NamedPlan.fields`), but it cannot name
intermediate columns without the mapping.

### 6.1 Getting the mapping out

**Recommended: stash it in the session extension, only when lineage is enabled.**
When `lineage.enabled` is set, `PlanResolver` writes the `#N → name` map into
`LineageService`, keyed by operation ID; the extractor reads it there and drops it when the
run reaches a terminal state. Zero allocation when the feature is off, no core type widened,
no signature changed. The cost is a side channel — the map travels out-of-band rather than
with the plan it describes — which is less elegant but matches how `JobService` and
`ActivityTracker` already carry per-session state.

**Alternative: widen `NamedPlan` and `resolve_and_execute_plan`.**

```rust
// Sketch — not compiled.
pub struct NamedPlan {
    pub plan: LogicalPlan,
    pub fields: Option<Vec<String>>,
    /// `#N` → user-facing name.
    pub field_names: Arc<HashMap<String, String>>,
}
```

`FieldInfo` (`state.rs:14`) already stores `name`, `plan_ids` and `hidden`, so the map is a
projection of existing data, and `FieldInfo::is_hidden()` conveniently marks the synthetic
join/sort helper columns that should be excluded from lineage. But this allocates on every
query whether or not lineage is on, and it changes `resolve_and_execute_plan`'s signature
(currently `(Arc<dyn ExecutionPlan>, Vec<StringifiedPlan>)`) across five call sites in two
crates. That is an invasive change to the hottest API in the codebase in service of an
off-by-default observability feature, and a reviewer would be right to push back. Take this
route only if the side channel proves unworkable.

### 6.2 Which plan to extract from is an open decision

Not settled, and the tradeoff is real:

- **Optimized plan** (what Spark's integration uses): reflects what actually executed.
  But projection pushdown, partition pruning and decorrelation remove columns and scans the
  user asked for, so the lineage under-reports intent.
- **Resolved plan** (pre-optimization): reflects what the user asked for, which is usually
  what a lineage consumer wants to see, and is where `PlanResolverState` is still alive.
  But it over-reports — it claims columns were read that the engine never touched.

Since §5.1's verification shows scans and write nodes survive optimization either way, this
is a semantics choice rather than a feasibility one. Worth deciding deliberately, and worth
stating in user documentation whichever way it goes. Leaning toward the resolved plan for
*column* lineage (intent) and the optimized plan for *dataset* lineage (fact), if the
inconsistency can be explained clearly — otherwise pick one.

### 6.3 Expression extraction

Mechanical: walk each `Projection`/`Aggregate`/`Window` and, for every output `Expr`, collect
`Expr::Column` references via `Expr::column_refs()`, distinguishing `DIRECT` transformations
(a bare column or a cast) from `INDIRECT` ones (filter/join/group-by predicates), as the
`columnLineage` facet requires.

---

## 7. Facets worth emitting

**Standard:**

| Facet | Source in Sail |
| --- | --- |
| `schema` (dataset) | `TableSource::schema()` / write node input schema |
| `dataSource` (dataset) | the canonicalized URI from §5.2 |
| `symlinks` (dataset) | `TableScan.table_name` and `LakehouseExecutionContext::catalog_table()` |
| `columnLineage` (output dataset) | §6 |
| `lifecycleStateChange` (output dataset) | `SinkMode` |
| `version` (dataset) | `DeltaSnapshot::version()`; Iceberg snapshot ID |
| `outputStatistics` (output dataset) | physical plan `MetricsSet` after execution — see below |
| `parent` (run) | session run ID from `SparkSession::session_id()` |
| `errorMessage` (run) | `SparkError` / `PlanError` on the failure path |
| `processing_engine` (run) | `sail`, `env!("CARGO_PKG_VERSION")`, `openlineage_adapter_version` |
| `sql` (job) | original SQL text, available for `handle_execute_sql_command` |

**Sail-specific (prefixed `sail_`):**

- `sail_executionMode` — `local` / `local-cluster` / `kubernetes-cluster` from `AppConfig::mode`.
- `sail_pathPattern` — the glob pattern for a listing source, per §5.2.
- `sail_physicalPlan` — the `FinalPhysicalPlan` string that `resolve_and_execute_plan`
  already builds (`sail-plan/src/lib.rs:61`). Opt-in; plan strings are large and can contain
  literal values from the query.
- `sail_jobMetrics` — stage/partition counts from `JobService`, which already tracks jobs,
  stages and workers for the system tables (`crates/sail-common-datafusion/src/session/job.rs:260`).

**`outputStatistics` is harder than it looks.** It needs row and byte counts from the *sink*,
after execution. Three obstacles:

1. The `Lazy` path completes inside a detached `tokio::spawn`ed `Executor::run`
   (`crates/sail-spark-connect/src/executor.rs:261`) that does not hold the `ExecutionPlan`,
   so the plan `Arc` must be captured and carried to the completion point deliberately.
2. `TracingExec` (`crates/sail-telemetry/src/execution/physical_plan.rs:54`) already harvests
   `MetricsSet` per operator, but it is only injected when telemetry is enabled, and it feeds
   OTLP rather than a per-run aggregate. Reusing it couples lineage to a second feature flag.
3. In cluster mode the sink runs on workers; whether the driver sees aggregated sink metrics
   needs checking before promising this facet at all.

Treat `outputStatistics` as Phase 2+ and do not let the rest of the design depend on it.

---

## 8. Proposed shape

### 8.1 Crate layout

```
crates/sail-lineage/
  src/
    lib.rs
    config.rs        // LineageConfig ← AppConfig
    service.rs       // LineageService: SessionExtension (also holds the #N map, §6.1)
    event.rs         // RunEvent model + facets (serde)
    naming.rs        // URL → (namespace, name), canonicalization, §2 + §5.2
    identity.rs      // job-name derivation and emit filtering, §4
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
`reqwest` — and deliberately **not** on the format crates, which is what the per-format
`LineageExtractor` list in §5.1 buys.

Wiring: register `LineageService` in `ServerSessionFactory::create_session_config`
(`crates/sail-session/src/session_factory/server.rs:124`), alongside `JobService` and
`ActivityTracker`, and compose the extractor list next to `create_table_format_registry()`.

### 8.2 Emission must never block or fail a query

Non-negotiable. The transport should own a bounded `tokio::sync::mpsc` channel and a
background task; `emit()` does a `try_send` and increments a dropped-event counter on a full
queue. A lineage backend being down, slow or misconfigured must degrade to a log line.
`sail-telemetry`'s `BatchLogProcessor` setup (`crates/sail-telemetry/src/telemetry.rs`) is
the right model.

The workspace lints help here: `unwrap_used`, `expect_used` and `panic` are all `deny` at the
workspace level (`Cargo.toml:17-23`).

### 8.3 Configuration

Following the declarative pattern in `crates/sail-common/src/config/application.yaml`
(a YAML definition plus a matching `serde` struct in `application.rs`):

```yaml
- key: lineage.enabled
  type: boolean
  default: "false"
  description: Whether to emit OpenLineage events for query executions.
  experimental: true

- key: lineage.emit
  type: string
  default: "writes"
  description: |
    Which executions produce OpenLineage events.
    `writes` emits only for executions that write a dataset.
    `queries` additionally emits read-only executions, with the job name derived
    from a hash of the query plan.
    `all` emits every execution. `all` can create a large number of single-run
    jobs in the lineage catalog for interactive sessions.
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

- key: lineage.job_name_prefix
  type: string
  default: ""
  description: |
    The prefix for OpenLineage job names, identifying the application across
    sessions. Defaults to the Spark application name, or `sail` if unset.
    This must not vary between runs of the same workload, or each run creates
    a new job in the lineage catalog.
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

## 9. Prior art and the build-vs-depend question

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
  to it without the per-format extraction in §5 anyway.
- It cannot resolve `#N` column IDs (§6), so its column lineage would be nameless on Sail.
- It hooks planning only, so it has no view of the `Executor` lifecycle and no answer to the
  job-identity and volume problems in §4.
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

## 10. Gaps and open questions

1. **Streaming.** `handle_execute_write_stream_operation_start` starts a long-running query.
   OpenLineage models this as one run with periodic `RUNNING` events, but Sail's streaming
   layer (`crates/sail-plan/src/streaming/`) has no per-micro-batch callback today. Phase 3
   at the earliest; the design should not assume runs are short.
2. **Cluster mode.** In `KubernetesCluster` mode, work is distributed across workers. Events
   should be emitted **only from the driver** — `JobService` already has the driver-side
   view, and worker sessions (`WorkerSessionFactory`) must not register a `LineageService`.
   Whether the driver can see sink-level metrics for `outputStatistics` (§7) is unresolved.
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
   validation against the published `OpenLineage.json` schema. Add the format-coverage test
   from §5.1 and canonicalization unit tests from §5.2.

---

## 11. Phasing

| Phase | Deliverable | Rough shape |
| --- | --- | --- |
| **0** | Spike: per-format `LineageExtractor` impls + a visitor over the final logical plan, printing to stdout. No transport, no config, no job naming. Answers "does the plan carry what we need, and for how many real queries?" | small, throwaway |
| **1** | Table-level lineage. `sail-lineage` crate, `LineageService`, config, job-identity and emit filtering (§4), HTTP + console transports, `START`/`COMPLETE`/`FAIL` from `handle_execute_plan`, `schema`/`dataSource`/`symlinks`/`lifecycleStateChange` facets, Marquez end-to-end test. | the real milestone |
| **2** | Column-level lineage via §6.1, and the §6.2 decision. `outputStatistics` if §7's obstacles clear. Parent/child run correlation. | depends on Phase 1 |
| **3** | Streaming runs, cluster-mode job facets, Flight SQL frontend, user documentation under `docs/guide/integrations/`. | long tail |

Phase 1 is the point at which Sail becomes usable with Marquez, DataHub, Atlan, Astronomer
and every other OpenLineage consumer. Phase 2 is what makes it competitive with Spark's
integration rather than merely equivalent.

---

## 12. Smallest honest next step

Phase 0, as one throwaway binary:

1. Add a `LineageExtractor` impl in `sail-data-source`, `sail-delta-lake` and `sail-iceberg`,
   each doing its own concrete downcast, mirroring the existing `*PhysicalPlanner` types.
2. Write a `LogicalPlan` visitor that collects inputs and outputs and prints them.
3. Call it from `resolve_and_execute_plan` behind `SAIL_LINEAGE__ENABLED`.
4. Run the existing Spark test suite against it and record, per query: did it find the
   datasets, and would §4.3's default have emitted anything at all?

Step 4 is the point of the spike. It measures two things this note can only guess at: the
coverage gap (how many plans produce datasets the extractor cannot name) and the volume
profile (what fraction of real executions are read-only). The second one decides whether §4's
default is right, and that is the decision most expensive to get wrong later.

---

## References

- [OpenLineage spec (`OpenLineage.json`)](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.json)
- [OpenLineage naming conventions](https://openlineage.io/docs/spec/naming/)
- [OpenLineage custom facets](https://openlineage.io/docs/spec/facets/custom-facets/)
- [`openlineage-client` crate](https://crates.io/crates/openlineage-client)
- [`datafusion-openlineage` crate](https://crates.io/crates/datafusion-openlineage)
- [`open-lakehouse/headwaters`](https://github.com/open-lakehouse/headwaters)
- [OpenLineage SQL integration (Rust)](https://github.com/OpenLineage/OpenLineage/tree/main/integration/sql)
