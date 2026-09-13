// https://github.com/delta-io/delta-rs/blob/5575ad16bf641420404611d65f4ad7626e9acb16/LICENSE.txt
//
// Copyright (2020) QP Hou and a number of other contributors.
// Portions Copyright (2025) LakeSail, Inc.
// Modified in 2025 by LakeSail, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [Credit]: <https://github.com/delta-io/delta-rs/blob/3607c314cbdd2ad06c6ee0677b92a29f695c71f3/crates/core/src/delta_datafusion/mod.rs>

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{
    DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef,
};
use datafusion::catalog::Session;
use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, ParquetSource, wrap_partition_type_in_dict,
    wrap_partition_value_in_dict,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr};
use object_store::path::Path;
use sail_common_datafusion::schema_evolution::{
    SchemaEvolutionPhysicalExprAdapterFactoryWithMatching, StructFieldMatching,
};
use sail_common_datafusion::variant::with_variant_extension_if_marked_storage;

use crate::conversion::ScalarConverter;
use crate::datasource::pruning::{arrow_type_contains_timestamp, widen_timestamp_max_scalar};
use crate::datasource::{DeltaScanConfig, create_object_store_url, partitioned_file_from_action};
use crate::delta_log::LogStoreRef;
use crate::schema::arrow_field_physical_name;
use crate::spec::{Add, ColumnMappingMode};
use crate::table::DeltaSnapshot;

/// Parameters for building file scan configuration
pub struct FileScanParams<'a> {
    pub projection: Option<&'a Vec<usize>>,
    pub limit: Option<usize>,
    pub pushdown_filter: Option<Arc<dyn PhysicalExpr>>,
    pub sort_order: Option<LexOrdering>,
    /// How to populate table-level statistics for the scan.
    ///
    /// This is separate from per-file statistics attached to each [`PartitionedFile`].
    pub table_stats_mode: TableStatsMode,
}

/// Strategy for providing table-level statistics to DataFusion.
#[derive(Debug, Clone, Copy)]
pub enum TableStatsMode {
    /// Use snapshot/log-derived statistics for the provided `Add` actions.
    Snapshot,
    /// Aggregate statistics only from the provided `Add` actions (chunk-local).
    AddsOnly,
    /// Do not compute statistics; return unknown stats.
    Unknown,
}

pub(crate) fn physical_to_logical_name_map(snapshot: &DeltaSnapshot) -> HashMap<String, String> {
    let column_mapping_mode = snapshot.effective_column_mapping_mode();
    let mut physical_to_logical = HashMap::new();
    for field in snapshot.schema().fields() {
        let logical = field.name().clone();
        let physical = arrow_field_physical_name(field, column_mapping_mode).to_string();
        physical_to_logical.entry(physical).or_insert(logical);
    }
    physical_to_logical
}

pub(crate) fn file_scan_logical_names(
    snapshot: &DeltaSnapshot,
    scan_config: &DeltaScanConfig,
    file_schema: &SchemaRef,
) -> Vec<String> {
    let physical_to_logical = physical_to_logical_name_map(snapshot);
    let mut names = file_schema
        .fields()
        .iter()
        .map(|field| {
            physical_to_logical
                .get(field.name())
                .cloned()
                .unwrap_or_else(|| field.name().clone())
        })
        .collect::<Vec<_>>();
    names.extend(snapshot.metadata().partition_columns().iter().cloned());
    if let Some(file_column_name) = &scan_config.file_column_name {
        names.push(file_column_name.clone());
    }
    if let Some(commit_version_column_name) = &scan_config.commit_version_column_name {
        names.push(commit_version_column_name.clone());
    }
    if let Some(commit_timestamp_column_name) = &scan_config.commit_timestamp_column_name {
        names.push(commit_timestamp_column_name.clone());
    }
    names
}

fn logical_file_schema_for_scan(
    physical_file_schema: &SchemaRef,
    logical_table_schema: &SchemaRef,
    column_mapping_mode: ColumnMappingMode,
) -> SchemaRef {
    let logical_fields_by_physical_name = logical_table_schema
        .fields()
        .iter()
        .map(|field| {
            (
                arrow_field_physical_name(field, column_mapping_mode).to_string(),
                Arc::clone(field),
            )
        })
        .collect::<HashMap<_, _>>();
    let fields = physical_file_schema
        .fields()
        .iter()
        .map(|physical_field| {
            logical_fields_by_physical_name
                .get(physical_field.name())
                .map(|logical_field| {
                    Arc::new(
                        with_variant_extension_if_marked_storage(logical_field.as_ref().clone())
                            .with_name(physical_field.name()),
                    )
                })
                .unwrap_or_else(|| Arc::clone(physical_field))
        })
        .collect::<Vec<_>>();

    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        physical_file_schema.metadata().clone(),
    ))
}

pub(crate) fn file_scan_projection_for_schema(
    snapshot: &DeltaSnapshot,
    scan_config: &DeltaScanConfig,
    file_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<Vec<usize>> {
    let scan_names = file_scan_logical_names(snapshot, scan_config, file_schema);
    output_schema
        .fields()
        .iter()
        .map(|field| {
            scan_names
                .iter()
                .position(|name| name == field.name())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Column '{}' is not available in Delta file scan schema",
                        field.name()
                    ))
                })
        })
        .collect()
}

/// Spread `files` across up to `target_partitions` file groups.
///
/// One [`FileGroup`] becomes one execution partition, so the group count is the scan's
/// parallelism. Grouping by partition value instead — as this previously did — tied that
/// parallelism to the number of distinct Hive partitions selected, which collapsed a
/// single-partition read onto one task no matter how many files it covered or how many cores
/// were available. Delta's deletion-vector path already round-robins its `Add` actions the same
/// way (see `build_eager_adds_input`).
///
/// Files are distributed round-robin in input order, so plans stay deterministic. Groups can
/// still be uneven when file sizes are skewed; balancing by `size` would improve that, at the
/// cost of reordering files within the scan.
///
/// Always returns at least one group: DataFusion sanity checks require a scan to report at
/// least one partition even when every file was pruned away.
/// See <https://github.com/apache/datafusion/issues/11322>.
fn split_files_into_groups(
    files: Vec<PartitionedFile>,
    target_partitions: usize,
) -> Vec<FileGroup> {
    let group_count = target_partitions.max(1).min(files.len());
    if group_count <= 1 {
        return vec![FileGroup::from(files)];
    }
    let mut groups = vec![Vec::new(); group_count];
    for (index, file) in files.into_iter().enumerate() {
        groups[index % group_count].push(file);
    }
    groups.into_iter().map(FileGroup::from).collect()
}

/// Build a FileScanConfig from pruned files and scan configuration
pub fn build_file_scan_config(
    snapshot: &DeltaSnapshot,
    log_store: &LogStoreRef,
    files: &[Add],
    scan_config: &DeltaScanConfig,
    params: FileScanParams<'_>,
    session: &dyn Session,
    file_schema: SchemaRef,
) -> Result<FileScanConfig> {
    // Get the complete schema that includes partition columns
    let complete_schema = match scan_config.schema.clone() {
        Some(schema) => schema,
        None => Arc::new(snapshot.schema().clone()),
    };
    let config = scan_config.clone();
    let partition_columns_mapped = snapshot.physical_partition_columns();
    let physical_to_logical = physical_to_logical_name_map(snapshot);
    let logical_file_schema = logical_file_schema_for_scan(
        &file_schema,
        &complete_schema,
        snapshot.effective_column_mapping_mode(),
    );

    // Collect the scanned files in a single deterministic list. They are spread across file
    // groups below; each `PartitionedFile` carries its own partition values, so a group does
    // not need to be homogeneous in them.
    let mut scanned_files: Vec<PartitionedFile> = Vec::with_capacity(files.len());

    // Collect per-file statistics while building `PartitionedFile`s so we can reuse them to
    // produce chunk-local table statistics without re-parsing JSON.
    let mut per_file_stats: Vec<Arc<Statistics>> = Vec::new();

    for action in files.iter() {
        // Files with deletion vectors are accepted: DV filtering is applied post-scan
        // by DeltaScanByAddsExec which tracks row indices and excludes deleted rows.
        // Note: the physical numRecords in stats is the total file record count
        // (not accounting for DV deletions), which is correct for scan planning.

        let mut part =
            partitioned_file_from_action(action, &partition_columns_mapped, &complete_schema)?;
        let action_stats = stats_for_add(action, &file_schema)?;
        if let Some(stats) = action_stats {
            per_file_stats.push(Arc::clone(&stats));
            part.statistics = Some(stats);
        }

        // Add file column if configured
        if config.file_column_name.is_some() {
            let partition_value = if config.wrap_partition_values {
                wrap_partition_value_in_dict(datafusion::common::scalar::ScalarValue::Utf8(Some(
                    action.path.clone(),
                )))
            } else {
                datafusion::common::scalar::ScalarValue::Utf8(Some(action.path.clone()))
            };
            part.partition_values.push(partition_value);
        }
        if config.commit_version_column_name.is_some() {
            part.partition_values
                .push(datafusion::common::scalar::ScalarValue::Int64(
                    action.commit_version,
                ));
        }
        if config.commit_timestamp_column_name.is_some() {
            part.partition_values
                .push(datafusion::common::scalar::ScalarValue::Int64(
                    action.commit_timestamp,
                ));
        }

        scanned_files.push(part);
    }

    // Rewrite file paths with table location prefix
    scanned_files.iter_mut().for_each(|file| {
        file.object_meta.location = rewrite_data_file_location(
            Path::from(log_store.config().location.path()),
            file.object_meta.location.clone(),
        );
    });

    // Build table partition columns schema
    let mut table_partition_cols_schema = Vec::with_capacity(partition_columns_mapped.len());
    for column in &partition_columns_mapped {
        let field = complete_schema
            .field_with_name(&column.logical_name)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "Partition column {} not found in schema",
                    column.logical_name
                ))
            })?;
        let corrected = if config.wrap_partition_values {
            match field.data_type() {
                ArrowDataType::Utf8
                | ArrowDataType::LargeUtf8
                | ArrowDataType::Binary
                | ArrowDataType::LargeBinary => {
                    wrap_partition_type_in_dict(field.data_type().clone())
                }
                _ => field.data_type().clone(),
            }
        } else {
            field.data_type().clone()
        };
        table_partition_cols_schema.push(Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(&column.physical_name)
                .with_data_type(corrected),
        ));
    }

    // Add file column to partition schema if configured
    if let Some(file_column_name) = &config.file_column_name {
        let field_name_datatype = if config.wrap_partition_values {
            wrap_partition_type_in_dict(ArrowDataType::Utf8)
        } else {
            ArrowDataType::Utf8
        };
        table_partition_cols_schema.push(Arc::new(Field::new(
            file_column_name.clone(),
            field_name_datatype,
            true,
        )));
    }
    if let Some(commit_version_column_name) = &config.commit_version_column_name {
        table_partition_cols_schema.push(Arc::new(Field::new(
            commit_version_column_name.clone(),
            ArrowDataType::Int64,
            true,
        )));
    }
    if let Some(commit_timestamp_column_name) = &config.commit_timestamp_column_name {
        table_partition_cols_schema.push(Arc::new(Field::new(
            commit_timestamp_column_name.clone(),
            ArrowDataType::Int64,
            true,
        )));
    }

    // Configure Parquet source with pushdown filter
    let parquet_options = TableParquetOptions {
        global: session.config().options().execution.parquet.clone(),
        ..Default::default()
    };

    let table_schema = TableSchema::builder(logical_file_schema)
        .with_table_partition_cols(table_partition_cols_schema)
        .build();
    // Calculate table statistics.
    //
    // `Statistics::column_statistics` expects the same length as the table schema
    // (file schema + partition columns + optional virtual columns). If this vector is shorter,
    // projection statistics can panic when encountering a `Column` referring to a partition
    // column.
    let mut stats = match params.table_stats_mode {
        TableStatsMode::Snapshot => {
            let snapshot_schema = Arc::new(snapshot.schema().clone());
            snapshot
                .datafusion_table_statistics_for_adds(files)
                .map(|stats| {
                    map_statistics_to_schema_with_name_mapping(
                        &stats,
                        &snapshot_schema,
                        table_schema.table_schema(),
                        Some(&physical_to_logical),
                    )
                })
                .unwrap_or_else(|| {
                    datafusion::common::stats::Statistics::new_unknown(
                        table_schema.table_schema().as_ref(),
                    )
                })
        }
        TableStatsMode::AddsOnly => {
            // Compute stats only for the current `files` slice to match chunked execution.
            // If any file is missing stats, fall back to unknown rather than mixing partial
            // aggregates (which can be misleading for the optimizer).
            let all_have_stats = per_file_stats.len() == files.len();
            if all_have_stats {
                aggregate_table_stats_from_files(&per_file_stats)
            } else {
                datafusion::common::stats::Statistics::new_unknown(
                    table_schema.table_schema().as_ref(),
                )
            }
        }
        TableStatsMode::Unknown => {
            datafusion::common::stats::Statistics::new_unknown(table_schema.table_schema().as_ref())
        }
    };
    let expected_cols = table_schema.table_schema().fields().len();
    if stats.column_statistics.len() < expected_cols {
        stats.column_statistics.extend(
            (0..(expected_cols - stats.column_statistics.len()))
                .map(|_| ColumnStatistics::new_unknown()),
        );
    } else if stats.column_statistics.len() > expected_cols {
        stats.column_statistics.truncate(expected_cols);
    }

    sanitize_statistics_for_schema(table_schema.table_schema(), &mut stats);

    let mut parquet_source =
        ParquetSource::new(table_schema).with_table_parquet_options(parquet_options);

    if let Some(predicate) = params.pushdown_filter
        && config.enable_parquet_pushdown
    {
        parquet_source = parquet_source.with_predicate(predicate);
    }

    let file_source: Arc<dyn datafusion::datasource::physical_plan::FileSource> =
        Arc::new(parquet_source);

    // Build the final FileScanConfig
    let object_store_url = create_object_store_url(&log_store.config().location)?;
    let all_have_stats = scanned_files.iter().all(|file| file.has_statistics());
    let file_groups = match &params.sort_order {
        // An ordered scan must keep the ordering-preserving split, which decides group
        // membership from file statistics rather than from a parallelism target.
        Some(sort_order) if all_have_stats => FileScanConfig::split_groups_by_statistics(
            &file_schema,
            &[FileGroup::from(scanned_files)],
            sort_order,
        )?,
        _ => split_files_into_groups(scanned_files, session.config().target_partitions()),
    };

    let file_scan_config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(file_groups)
        .with_statistics(stats)
        .with_projection_indices(params.projection.cloned())?
        .with_limit(params.limit)
        .with_expr_adapter(Some(Arc::new(
            SchemaEvolutionPhysicalExprAdapterFactoryWithMatching::new_relaxed_timezone(
                match snapshot.effective_column_mapping_mode() {
                    crate::spec::ColumnMappingMode::None => StructFieldMatching::Name,
                    crate::spec::ColumnMappingMode::Name => StructFieldMatching::PhysicalName,
                    crate::spec::ColumnMappingMode::Id => StructFieldMatching::FieldId,
                },
            ),
        )))
        .build();

    Ok(file_scan_config)
}

fn aggregate_table_stats_from_files(file_stats: &[Arc<Statistics>]) -> Statistics {
    let mut num_rows = Precision::Exact(0usize);
    let mut column_statistics: Option<Vec<ColumnStatistics>> = None;

    for s in file_stats {
        num_rows = match (num_rows, s.num_rows) {
            (Precision::Exact(a), Precision::Exact(b)) => Precision::Exact(a.saturating_add(b)),
            _ => Precision::Absent,
        };

        match (&mut column_statistics, s.column_statistics.as_slice()) {
            (None, cols) => column_statistics = Some(cols.to_vec()),
            (Some(acc), cols) => {
                let n = acc.len().min(cols.len());
                for i in 0..n {
                    acc[i] = add_column_statistics(&acc[i], &cols[i]);
                }
            }
        }
    }

    Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics: column_statistics.unwrap_or_default(),
    }
}

fn add_column_statistics(a: &ColumnStatistics, b: &ColumnStatistics) -> ColumnStatistics {
    ColumnStatistics {
        null_count: a.null_count.add(&b.null_count),
        max_value: merge_max_bounds(&a.max_value, &b.max_value),
        min_value: merge_min_bounds(&a.min_value, &b.min_value),
        sum_value: Precision::Absent,
        distinct_count: a.distinct_count.add(&b.distinct_count),
        byte_size: a.byte_size.add(&b.byte_size),
    }
}

pub(crate) fn sanitize_statistics_for_schema(schema: &SchemaRef, stats: &mut Statistics) {
    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(column_stats) = stats.column_statistics.get_mut(idx) {
            sanitize_column_statistics_for_field(column_stats, field.name(), field.data_type());
        }
    }
}

fn sanitize_column_statistics_for_field(
    column_stats: &mut ColumnStatistics,
    _column_name: &str,
    data_type: &ArrowDataType,
) {
    column_stats.min_value = sanitize_bound_for_type(&column_stats.min_value, data_type);
    column_stats.max_value = sanitize_bound_for_type(&column_stats.max_value, data_type);

    let min_type = column_stats
        .min_value
        .get_value()
        .map(ScalarValue::data_type);
    let max_type = column_stats
        .max_value
        .get_value()
        .map(ScalarValue::data_type);
    if let (Some(min_type), Some(max_type)) = (min_type, max_type)
        && min_type != max_type
    {
        column_stats.min_value = Precision::Absent;
        column_stats.max_value = Precision::Absent;
        return;
    }
    if let (Some(min), Some(max)) = (
        column_stats.min_value.get_value(),
        column_stats.max_value.get_value(),
    ) && matches!(min.partial_cmp(max), Some(Ordering::Greater))
    {
        column_stats.min_value = Precision::Absent;
        column_stats.max_value = Precision::Absent;
    }
}

pub(crate) fn map_statistics_to_schema(
    statistics: &Statistics,
    source_schema: &SchemaRef,
    target_schema: &SchemaRef,
) -> Statistics {
    map_statistics_to_schema_with_name_mapping(statistics, source_schema, target_schema, None)
}

pub(crate) fn map_statistics_to_schema_with_name_mapping(
    statistics: &Statistics,
    source_schema: &SchemaRef,
    target_schema: &SchemaRef,
    target_to_source_names: Option<&HashMap<String, String>>,
) -> Statistics {
    let column_statistics = target_schema
        .fields()
        .iter()
        .map(|field| {
            let source_index = target_to_source_names
                .and_then(|names| names.get(field.name()))
                .and_then(|name| source_schema.index_of(name).ok())
                .or_else(|| source_schema.index_of(field.name()).ok());
            let mut column_statistics = source_index
                .and_then(|idx| statistics.column_statistics.get(idx).cloned())
                .unwrap_or_else(ColumnStatistics::new_unknown);
            sanitize_column_statistics_for_field(
                &mut column_statistics,
                field.name(),
                field.data_type(),
            );
            column_statistics
        })
        .collect();

    Statistics {
        num_rows: statistics.num_rows,
        total_byte_size: statistics.total_byte_size,
        column_statistics,
    }
}

fn sanitize_bound_for_type(
    bound: &Precision<ScalarValue>,
    data_type: &ArrowDataType,
) -> Precision<ScalarValue> {
    let sanitized = bound.cast_to(data_type).unwrap_or(Precision::Absent);
    if sanitized.get_value().is_some_and(ScalarValue::is_null) {
        Precision::Absent
    } else {
        sanitized
    }
}

fn merge_max_bounds(
    a: &Precision<ScalarValue>,
    b: &Precision<ScalarValue>,
) -> Precision<ScalarValue> {
    if bounds_have_mismatched_types(a, b) {
        Precision::Absent
    } else {
        a.max(b)
    }
}

fn merge_min_bounds(
    a: &Precision<ScalarValue>,
    b: &Precision<ScalarValue>,
) -> Precision<ScalarValue> {
    if bounds_have_mismatched_types(a, b) {
        Precision::Absent
    } else {
        a.min(b)
    }
}

fn bounds_have_mismatched_types(a: &Precision<ScalarValue>, b: &Precision<ScalarValue>) -> bool {
    let lhs = match a {
        Precision::Exact(v) | Precision::Inexact(v) => Some(v),
        Precision::Absent => None,
    };
    let rhs = match b {
        Precision::Exact(v) | Precision::Inexact(v) => Some(v),
        Precision::Absent => None,
    };

    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => lhs.data_type() != rhs.data_type(),
        _ => false,
    }
}

fn rewrite_data_file_location(table_root: Path, location: Path) -> Path {
    let raw = location.as_ref();
    if looks_like_absolute_uri(raw) {
        return location;
    }

    table_root.parts().chain(location.parts()).collect()
}

fn looks_like_absolute_uri(path: &str) -> bool {
    let Some((scheme, rest)) = path.split_once(':') else {
        return false;
    };
    !scheme.is_empty()
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        && rest.starts_with('/')
}

fn stats_for_add(action: &Add, file_schema: &SchemaRef) -> Result<Option<Arc<Statistics>>> {
    let stats = action
        .get_stats()
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let Some(stats) = stats else {
        return Ok(None);
    };

    let mut column_statistics = Vec::with_capacity(file_schema.fields().len());
    for field in file_schema.fields() {
        let field_name = field.name();
        let bound = |value| {
            if stats.tight_bounds {
                Precision::Exact(value)
            } else {
                Precision::Inexact(value)
            }
        };
        let mut min_value = stats
            .min_values
            .get(field_name)
            .and_then(|value| {
                ScalarConverter::column_value_stat_to_arrow_scalar_value(value, field.data_type())
                    .ok()
                    .flatten()
            })
            .filter(|value| !value.is_null())
            .map(bound)
            .unwrap_or(Precision::Absent);
        let mut max_value = stats
            .max_values
            .get(field_name)
            .and_then(|value| {
                ScalarConverter::column_value_stat_to_arrow_scalar_value(value, field.data_type())
                    .ok()
                    .flatten()
            })
            .filter(|value| !value.is_null())
            .map(bound)
            .unwrap_or(Precision::Absent);
        let null_count = stats
            .null_count_value(field_name)
            .map(|value| {
                if stats.tight_bounds {
                    Precision::Exact(value.max(0) as usize)
                } else {
                    Precision::Inexact(value.max(0) as usize)
                }
            })
            .unwrap_or(Precision::Absent);

        if arrow_type_contains_timestamp(field.data_type()) {
            min_value = min_value.to_inexact();
            max_value = max_value.map(widen_timestamp_max_scalar).to_inexact();
        }

        column_statistics.push(ColumnStatistics {
            null_count,
            max_value,
            min_value,
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        });
    }

    let num_rows = if stats.num_records >= 0 {
        Precision::Exact(stats.num_records as usize)
    } else {
        Precision::Absent
    };

    Ok(Some(Arc::new(Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics,
    })))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::common::stats::{ColumnStatistics, Precision, Statistics};
    use object_store::path::Path;

    use super::{
        add_column_statistics, build_file_scan_config, map_statistics_to_schema,
        map_statistics_to_schema_with_name_mapping, rewrite_data_file_location,
        sanitize_statistics_for_schema, stats_for_add,
    };
    use crate::conversion::ScalarConverter;
    use crate::spec::Add;

    /// Build `count` data files that all live in the same Hive partition (`p=1`).
    fn adds_in_one_partition(count: usize) -> Vec<Add> {
        (0..count)
            .map(|index| Add {
                path: format!("p=1/part-{index:05}.parquet"),
                partition_values: HashMap::from([("p".to_string(), Some("1".to_string()))]),
                size: 16 * 1024 * 1024,
                modification_time: 0,
                data_change: true,
                ..Default::default()
            })
            .collect()
    }

    fn one_partition_scan_config(
        adds: &[Add],
        target_partitions: usize,
    ) -> datafusion::datasource::physical_plan::FileScanConfig {
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::prelude::SessionConfig;
        use object_store::ObjectStore;
        use object_store::memory::InMemory;
        use url::Url;

        use crate::StorageConfig;
        use crate::datasource::DeltaScanConfig;
        use crate::datasource::scan::{FileScanParams, TableStatsMode};
        use crate::delta_log::default_logstore;
        use crate::snapshot::DeltaSnapshot;
        use crate::spec::{DataType as DeltaDataType, Metadata, StructField, StructType};

        #[expect(clippy::expect_used)]
        let schema = StructType::try_new(vec![
            StructField::new("p", DeltaDataType::STRING, true),
            StructField::new("v", DeltaDataType::LONG, true),
        ])
        .expect("schema");
        #[expect(clippy::expect_used)]
        let metadata =
            Metadata::try_new(None, None, schema, vec!["p".to_string()], 0, HashMap::new())
                .expect("metadata");
        #[expect(clippy::expect_used)]
        let snapshot = DeltaSnapshot::new_for_test(metadata, adds.to_vec()).expect("snapshot");

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        #[expect(clippy::expect_used)]
        let url = Url::parse("memory:///").expect("url");
        let log_store = default_logstore(store.clone(), store, &url, &StorageConfig);

        let session = SessionStateBuilder::new()
            .with_config(SessionConfig::new().with_target_partitions(target_partitions))
            .with_default_features()
            .build();

        // Non-partition columns only: this is the schema of the data inside each parquet file.
        let file_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));

        #[expect(clippy::expect_used)]
        build_file_scan_config(
            &snapshot,
            &log_store,
            adds,
            &DeltaScanConfig::default(),
            FileScanParams {
                projection: None,
                limit: None,
                pushdown_filter: None,
                sort_order: None,
                table_stats_mode: TableStatsMode::AddsOnly,
            },
            &session,
            file_schema,
        )
        .expect("file scan config")
    }

    /// Scan parallelism must follow `target_partitions`, not how the selected files happen to
    /// be spread over Hive partitions. Group membership is allowed to mix partition values
    /// because each `PartitionedFile` carries its own.
    #[test]
    fn scan_parallelism_is_independent_of_partition_count() {
        // Same 400 files, same target_partitions, only the partition-value spread differs.
        for distinct_partitions in [1usize, 4, 40] {
            let adds = (0..400usize)
                .map(|index| {
                    let partition = index % distinct_partitions;
                    Add {
                        path: format!("p={partition}/part-{index:05}.parquet"),
                        partition_values: HashMap::from([(
                            "p".to_string(),
                            Some(partition.to_string()),
                        )]),
                        size: 16 * 1024 * 1024,
                        modification_time: 0,
                        data_change: true,
                        ..Default::default()
                    }
                })
                .collect::<Vec<_>>();

            let config = one_partition_scan_config(&adds, 200);
            assert_eq!(
                config.file_groups.len(),
                200,
                "file group count must follow target_partitions, but {distinct_partitions} \
                 distinct partition value(s) produced {} group(s)",
                config.file_groups.len()
            );
            let total_files: usize = config.file_groups.iter().map(|group| group.len()).sum();
            assert_eq!(total_files, 400, "every file must still be scanned");
        }
    }

    /// Fewer files than `target_partitions` must not create empty groups, and a single file
    /// must not be split. Empty inputs still need one group for DataFusion's sanity checks.
    #[test]
    fn scan_file_groups_are_bounded_by_file_count() {
        for file_count in [0usize, 1, 7] {
            let adds = adds_in_one_partition(file_count);
            let config = one_partition_scan_config(&adds, 200);

            assert_eq!(
                config.file_groups.len(),
                file_count.max(1),
                "{file_count} file(s) should produce {} group(s)",
                file_count.max(1)
            );
            assert!(
                config.file_groups.iter().all(|group| group.len() <= 1),
                "no group should hold more than one file when files are scarce"
            );
        }
    }

    /// Reading a single Hive partition must still spread its files across execution
    /// partitions. Grouping files solely by partition value collapses the scan to one
    /// `FileGroup`, so the whole read runs on a single task regardless of `target_partitions`.
    #[test]
    fn single_hive_partition_scan_uses_target_partitions() {
        let adds = adds_in_one_partition(400);
        let config = one_partition_scan_config(&adds, 200);

        let total_files: usize = config.file_groups.iter().map(|group| group.len()).sum();
        assert_eq!(total_files, 400, "every file must still be scanned");
        assert_eq!(
            config.file_groups.len(),
            200,
            "400 files in one Hive partition should spread over target_partitions groups, \
             got {} group(s)",
            config.file_groups.len()
        );
    }

    #[test]
    fn test_scalar_from_json_null_returns_typed_null() {
        #[expect(clippy::unwrap_used)]
        let value =
            ScalarConverter::json_to_arrow_scalar_value(&serde_json::Value::Null, &DataType::Int64)
                .unwrap();
        assert_eq!(value, Some(ScalarValue::Int64(None)));
    }

    #[test]
    fn test_add_column_statistics_absents_mismatched_bounds() {
        let lhs = ColumnStatistics {
            null_count: Precision::Absent,
            max_value: Precision::Exact(ScalarValue::Null),
            min_value: Precision::Exact(ScalarValue::Null),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        };
        let rhs = ColumnStatistics {
            null_count: Precision::Absent,
            max_value: Precision::Exact(ScalarValue::Int64(Some(5))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        };

        let merged = add_column_statistics(&lhs, &rhs);
        assert_eq!(merged.max_value, Precision::Absent);
        assert_eq!(merged.min_value, Precision::Absent);
    }

    #[test]
    fn test_sanitize_statistics_for_schema_removes_untyped_null_bounds() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let mut stats = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(5))),
                min_value: Precision::Exact(ScalarValue::Null),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };

        sanitize_statistics_for_schema(&schema, &mut stats);

        assert_eq!(
            stats.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int64(Some(5)))
        );
        assert_eq!(stats.column_statistics[0].min_value, Precision::Absent);
    }

    #[test]
    fn test_sanitize_statistics_for_schema_removes_invalid_min_max() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let mut stats = Statistics {
            num_rows: Precision::Exact(2),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int32(Some(22))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(3))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };

        sanitize_statistics_for_schema(&schema, &mut stats);

        assert_eq!(stats.column_statistics[0].min_value, Precision::Absent);
        assert_eq!(stats.column_statistics[0].max_value, Precision::Absent);
    }

    #[test]
    fn test_map_statistics_to_schema_reorders_partition_columns_by_name() {
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("load_date", DataType::Date32, true),
            Field::new("id", DataType::Utf8, true),
            Field::new("payload_column_1", DataType::Int32, true),
            Field::new("some_col", DataType::Utf8, true),
        ]));
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("payload_column_1", DataType::Int32, true),
            Field::new("some_col", DataType::Utf8, true),
            Field::new("load_date", DataType::Date32, true),
        ]));
        let statistics = Statistics {
            num_rows: Precision::Exact(2),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown(),
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("3".to_string()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("2".to_string()))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Int32(Some(22))),
                    min_value: Precision::Exact(ScalarValue::Int32(Some(3))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("foo".to_string()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("foo".to_string()))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    byte_size: Precision::Absent,
                },
            ],
        };

        let mapped = map_statistics_to_schema(&statistics, &source_schema, &target_schema);

        assert_eq!(mapped.num_rows, Precision::Exact(2));
        assert_eq!(mapped.column_statistics.len(), 4);
        assert_eq!(
            mapped.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Utf8(Some("2".to_string())))
        );
        assert_eq!(
            mapped.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Utf8(Some("3".to_string())))
        );
        assert_eq!(
            mapped.column_statistics[1].min_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );
        assert_eq!(
            mapped.column_statistics[1].max_value,
            Precision::Exact(ScalarValue::Int32(Some(22)))
        );
        assert_eq!(
            mapped.column_statistics[2].min_value,
            Precision::Exact(ScalarValue::Utf8(Some("foo".to_string())))
        );
        assert_eq!(mapped.column_statistics[3], ColumnStatistics::new_unknown());
    }

    #[test]
    fn test_map_statistics_to_schema_supports_physical_column_names() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload_column_1",
            DataType::Int32,
            true,
        )]));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "col-physical-payload",
            DataType::Int32,
            true,
        )]));
        let statistics = Statistics {
            num_rows: Precision::Exact(2),
            total_byte_size: Precision::Absent,
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int32(Some(22))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(3))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Absent,
            }],
        };
        let physical_to_logical = HashMap::from([(
            "col-physical-payload".to_string(),
            "payload_column_1".to_string(),
        )]);

        let mapped = map_statistics_to_schema_with_name_mapping(
            &statistics,
            &source_schema,
            &target_schema,
            Some(&physical_to_logical),
        );

        assert_eq!(
            mapped.column_statistics[0].min_value,
            Precision::Exact(ScalarValue::Int32(Some(3)))
        );
        assert_eq!(
            mapped.column_statistics[0].max_value,
            Precision::Exact(ScalarValue::Int32(Some(22)))
        );
    }

    #[test]
    fn test_rewrite_data_file_location_preserves_absolute_uri_paths() {
        let table_root = Path::from("bucket/table");
        let absolute = Path::from("s3://other-bucket/path/part-000.parquet");

        let rewritten = rewrite_data_file_location(table_root, absolute.clone());

        assert_eq!(rewritten, absolute);
    }

    #[test]
    fn test_rewrite_data_file_location_prefixes_relative_paths() {
        let rewritten = rewrite_data_file_location(
            Path::from("bucket/table"),
            Path::from("part=1/part-000.parquet"),
        );

        assert_eq!(
            rewritten,
            Path::from("bucket/table/part=1/part-000.parquet")
        );
    }

    #[test]
    fn test_rewrite_data_file_location_preserves_percent_encoded_partition_dirs() {
        #[expect(clippy::expect_used)]
        let location =
            Path::parse("ts_utc=2024-01-15%2010%3A30%3A00.123456/part-00000-abc.snappy.parquet")
                .expect("valid path");

        let rewritten = rewrite_data_file_location(Path::from("bucket/table"), location);

        assert_eq!(
            rewritten.as_ref(),
            "bucket/table/ts_utc=2024-01-15%2010%3A30%3A00.123456/part-00000-abc.snappy.parquet"
        );
    }

    #[test]
    #[expect(clippy::expect_used, clippy::unwrap_used)]
    fn test_stats_for_add_marks_wide_bounds_as_inexact() {
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let add = Add {
            path: "part-000.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: Some(
                r#"{"numRecords":3,"tightBounds":false,"minValues":{"value":1},"maxValues":{"value":7},"nullCount":{"value":0}}"#
                    .to_string(),
            ),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        };

        let stats = stats_for_add(&add, &file_schema)
            .unwrap()
            .expect("stats should be present");
        let column = &stats.column_statistics[0];

        assert_eq!(
            column.min_value,
            Precision::Inexact(ScalarValue::Int32(Some(1)))
        );
        assert_eq!(
            column.max_value,
            Precision::Inexact(ScalarValue::Int32(Some(7)))
        );
        assert_eq!(column.null_count, Precision::Inexact(0));
    }

    #[test]
    #[expect(clippy::expect_used, clippy::unwrap_used)]
    fn test_stats_for_add_does_not_fallback_to_a_colliding_logical_name() {
        let file_schema = Arc::new(Schema::new(vec![Field::new(
            "col-target",
            DataType::Int64,
            true,
        )]));
        let add = Add {
            path: "part-000.parquet".to_string(),
            partition_values: HashMap::new(),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: Some(
                r#"{"numRecords":1,"minValues":{"col-source":0},"maxValues":{"col-source":0},"nullCount":{"col-source":0}}"#
                    .to_string(),
            ),
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
            commit_version: None,
            commit_timestamp: None,
        };
        let stats = stats_for_add(&add, &file_schema)
            .unwrap()
            .expect("stats should be present");
        let column = &stats.column_statistics[0];

        assert_eq!(column.min_value, Precision::Absent);
        assert_eq!(column.max_value, Precision::Absent);
        assert_eq!(column.null_count, Precision::Absent);
    }
}
