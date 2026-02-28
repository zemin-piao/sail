use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};

#[derive(Debug, Clone)]
pub enum ValueEncoding {
    Avro {
        reader_schema_json: String,
        schema_id: u32,
    },
    Json {
        schema: ArrowSchema,
    },
    Raw,
}

/// Spark-compatible raw Kafka output schema. All fields nullable=true per Spark defaults.
pub fn raw_kafka_schema(include_headers: bool) -> ArrowSchema {
    let mut fields = vec![
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new("topic", DataType::Utf8, true),
        Field::new("partition", DataType::Int32, true),
        Field::new("offset", DataType::Int64, true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("timestampType", DataType::Int32, true),
    ];
    if include_headers {
        fields.push(Field::new(
            "headers",
            DataType::List(
                Field::new_struct(
                    "item",
                    &[
                        Field::new("key", DataType::Utf8, true),
                        Field::new("value", DataType::Binary, true),
                    ],
                    true,
                )
                .into(),
            ),
            true,
        ));
    }
    ArrowSchema::new(fields)
}

pub fn kafka_metadata_fields() -> Vec<Field> {
    vec![
        Field::new("_kafka_key", DataType::Binary, true),
        Field::new("_kafka_topic", DataType::Utf8, true),
        Field::new("_kafka_partition", DataType::Int32, true),
        Field::new("_kafka_offset", DataType::Int64, true),
        Field::new(
            "_kafka_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]
}

pub fn build_deserialized_schema(value_schema: &ArrowSchema) -> ArrowSchema {
    let mut fields: Vec<Field> = value_schema
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.extend(kafka_metadata_fields());
    ArrowSchema::new(fields)
}
