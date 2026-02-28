use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    Boundedness, DisplayAs, DisplayFormatType, EmissionType, ExecutionPlan, Partitioning,
    PlanProperties,
};

use crate::encoding::ValueEncoding;
use crate::offsets::KafkaPartitionRange;
use crate::options::KafkaReadOptions;
use crate::stream::kafka_partition_stream;

#[derive(Debug)]
pub struct KafkaScanExec {
    schema: SchemaRef,
    encoding: ValueEncoding,
    partition_ranges: Vec<KafkaPartitionRange>,
    options: KafkaReadOptions,
    properties: PlanProperties,
}

impl KafkaScanExec {
    pub fn new(
        schema: SchemaRef,
        encoding: ValueEncoding,
        partition_ranges: Vec<KafkaPartitionRange>,
        options: KafkaReadOptions,
    ) -> Self {
        let num_partitions = partition_ranges.len();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            encoding,
            partition_ranges,
            options,
            properties,
        }
    }
}

impl DisplayAs for KafkaScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let topics: Vec<&str> = self
                    .partition_ranges
                    .iter()
                    .map(|r| r.topic.as_str())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect();
                write!(
                    f,
                    "KafkaScanExec: topics={:?}, partitions={}, encoding={:?}",
                    topics,
                    self.partition_ranges.len(),
                    match &self.encoding {
                        ValueEncoding::Avro { .. } => "avro",
                        ValueEncoding::Json { .. } => "json",
                        ValueEncoding::Raw => "raw",
                    }
                )
            }
        }
    }
}

impl ExecutionPlan for KafkaScanExec {
    fn name(&self) -> &'static str {
        "KafkaScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::common::DataFusionError::Internal(
                "KafkaScanExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let range = self.partition_ranges.get(partition).ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(format!(
                "partition index {partition} out of range (have {})",
                self.partition_ranges.len()
            ))
        })?;

        let stream = kafka_partition_stream(
            self.options.clone(),
            range.clone(),
            self.encoding.clone(),
            self.schema.clone(),
            8192,
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
