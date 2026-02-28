use datafusion::common::DataFusionError;

#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Kafka config error: {0}")]
    Config(String),
    #[error("Schema Registry error: {0}")]
    SchemaRegistry(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

impl From<KafkaError> for DataFusionError {
    fn from(err: KafkaError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

pub type KafkaResult<T> = Result<T, KafkaError>;
