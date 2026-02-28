use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::encoding::ValueEncoding;
use crate::offsets::KafkaPartitionRange;
use crate::options::KafkaReadOptions;
use crate::scan_exec::KafkaScanExec;

#[derive(Debug)]
pub struct KafkaTableProvider {
    schema: SchemaRef,
    encoding: ValueEncoding,
    partition_ranges: Vec<KafkaPartitionRange>,
    options: KafkaReadOptions,
}

impl KafkaTableProvider {
    pub fn new(
        schema: SchemaRef,
        encoding: ValueEncoding,
        partition_ranges: Vec<KafkaPartitionRange>,
        options: KafkaReadOptions,
    ) -> Self {
        Self {
            schema,
            encoding,
            partition_ranges,
            options,
        }
    }
}

#[async_trait]
impl TableProvider for KafkaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(KafkaScanExec::new(
            self.schema.clone(),
            self.encoding.clone(),
            self.partition_ranges.clone(),
            self.options.clone(),
        )))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
    }
}
