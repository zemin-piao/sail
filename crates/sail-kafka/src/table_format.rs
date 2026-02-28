use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{not_impl_err, Result};
use datafusion::physical_plan::ExecutionPlan;
use sail_common_datafusion::datasource::{
    SinkInfo, SourceInfo, TableFormat, TableFormatRegistry,
};

use crate::encoding::{build_deserialized_schema, raw_kafka_schema, ValueEncoding};
use crate::offsets::resolve_offset_ranges;
use crate::options::{KafkaReadOptions, ValueFormat};
use crate::provider::KafkaTableProvider;
use crate::schema_registry::{SchemaRegistryClient, SchemaRegistryConfig};

#[derive(Debug)]
pub struct KafkaTableFormat;

impl KafkaTableFormat {
    pub fn register(registry: &TableFormatRegistry) -> Result<()> {
        registry.register(Arc::new(Self))
    }
}

#[async_trait]
impl TableFormat for KafkaTableFormat {
    fn name(&self) -> &str {
        "kafka"
    }

    async fn create_provider(
        &self,
        _ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        let options = KafkaReadOptions::from_options(&info.options)?;

        let (schema, encoding) = resolve_schema_and_encoding(&options).await?;
        let partition_ranges = resolve_offset_ranges(&options)?;

        Ok(Arc::new(KafkaTableProvider::new(
            Arc::new(schema),
            encoding,
            partition_ranges,
            options,
        )))
    }

    async fn create_writer(
        &self,
        _ctx: &dyn Session,
        _info: SinkInfo,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("writing to Kafka is not supported")
    }
}

async fn resolve_schema_and_encoding(
    options: &KafkaReadOptions,
) -> Result<(arrow::datatypes::Schema, ValueEncoding)> {
    let explicit_format = options.value_format.clone();

    if let Some(registry_url) = &options.schema_registry_url {
        let basic_auth = parse_basic_auth(options);
        let client = SchemaRegistryClient::new(SchemaRegistryConfig {
            url: registry_url.clone(),
            basic_auth,
        })?;

        let topic = get_primary_topic(options);
        let subject = options
            .value_schema_subject
            .clone()
            .unwrap_or_else(|| format!("{topic}-value"));

        let registered = client.get_latest_version(&subject).await?;

        let format = explicit_format.unwrap_or_else(|| {
            match registered.schema_type.to_uppercase().as_str() {
                "JSON" => ValueFormat::Json,
                _ => ValueFormat::Avro,
            }
        });

        match format {
            ValueFormat::Avro => {
                let avro_schema: serde_json::Value =
                    serde_json::from_str(&registered.schema_json).map_err(|e| {
                        datafusion::common::DataFusionError::External(Box::new(
                            crate::error::KafkaError::SchemaRegistry(format!(
                                "invalid Avro schema JSON: {e}"
                            )),
                        ))
                    })?;
                let arrow_schema = avro_to_arrow_schema(&avro_schema)?;
                let output_schema = build_deserialized_schema(&arrow_schema);
                Ok((
                    output_schema,
                    ValueEncoding::Avro {
                        reader_schema_json: registered.schema_json,
                    },
                ))
            }
            ValueFormat::Json => {
                let json_schema: serde_json::Value =
                    serde_json::from_str(&registered.schema_json).map_err(|e| {
                        datafusion::common::DataFusionError::External(Box::new(
                            crate::error::KafkaError::SchemaRegistry(format!(
                                "invalid JSON schema: {e}"
                            )),
                        ))
                    })?;
                let arrow_schema = json_schema_to_arrow(&json_schema)?;
                let output_schema = build_deserialized_schema(&arrow_schema);
                Ok((output_schema, ValueEncoding::Json { schema: arrow_schema }))
            }
            ValueFormat::Raw => Ok((raw_kafka_schema(options.include_headers), ValueEncoding::Raw)),
        }
    } else {
        match explicit_format {
            Some(ValueFormat::Avro) => Err(datafusion::common::DataFusionError::Plan(
                "value.format=avro requires schema.registry.url".to_string(),
            )),
            Some(ValueFormat::Json) => {
                // JSON without schema registry uses raw mode
                // (schema can be inferred or provided via explicit schema option)
                Ok((raw_kafka_schema(options.include_headers), ValueEncoding::Raw))
            }
            _ => Ok((raw_kafka_schema(options.include_headers), ValueEncoding::Raw)),
        }
    }
}

fn get_primary_topic(options: &KafkaReadOptions) -> String {
    match &options.topic_selection {
        crate::options::TopicSelection::Subscribe(topics) => {
            topics.first().cloned().unwrap_or_default()
        }
        crate::options::TopicSelection::SubscribePattern(p) => p.clone(),
        crate::options::TopicSelection::Assign(map) => {
            map.keys().next().cloned().unwrap_or_default()
        }
    }
}

fn parse_basic_auth(options: &KafkaReadOptions) -> Option<(String, String)> {
    let merged: std::collections::HashMap<String, String> = options
        .kafka_properties
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    merged
        .get("schema.registry.basic.auth.user.info")
        .and_then(|v| {
            let parts: Vec<&str> = v.splitn(2, ':').collect();
            if parts.len() == 2 {
                Some((parts[0].to_string(), parts[1].to_string()))
            } else {
                None
            }
        })
}

fn avro_to_arrow_schema(
    avro_json: &serde_json::Value,
) -> Result<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, TimeUnit};

    let fields = avro_json["fields"]
        .as_array()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "Avro schema must be a record type with 'fields'".to_string(),
            )
        })?;

    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let name = f["name"].as_str().unwrap_or("unknown").to_string();
            let (dtype, nullable) = avro_type_to_arrow(&f["type"])?;
            Ok(Field::new(name, dtype, nullable))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(arrow::datatypes::Schema::new(arrow_fields))
}

fn avro_type_to_arrow(avro_type: &serde_json::Value) -> Result<(arrow::datatypes::DataType, bool)> {
    use arrow::datatypes::{DataType, Field, TimeUnit};

    match avro_type {
        serde_json::Value::String(s) => match s.as_str() {
            "null" => Ok((DataType::Null, true)),
            "boolean" => Ok((DataType::Boolean, false)),
            "int" => Ok((DataType::Int32, false)),
            "long" => Ok((DataType::Int64, false)),
            "float" => Ok((DataType::Float32, false)),
            "double" => Ok((DataType::Float64, false)),
            "bytes" => Ok((DataType::Binary, false)),
            "string" => Ok((DataType::Utf8, false)),
            other => Err(datafusion::common::DataFusionError::Plan(format!(
                "unsupported Avro type: {other}"
            ))),
        },
        serde_json::Value::Array(union_types) => {
            // Union type -- check for ["null", T] pattern (nullable)
            let non_null: Vec<_> = union_types
                .iter()
                .filter(|t| !matches!(t, serde_json::Value::String(s) if s == "null"))
                .collect();
            let has_null = union_types.len() > non_null.len();

            if non_null.len() == 1 {
                let (dtype, _) = avro_type_to_arrow(non_null[0])?;
                Ok((dtype, has_null))
            } else {
                // Complex union -- fall back to binary
                Ok((DataType::Binary, has_null))
            }
        }
        serde_json::Value::Object(obj) => {
            if let Some(logical_type) = obj.get("logicalType").and_then(|v| v.as_str()) {
                match logical_type {
                    "date" => return Ok((DataType::Date32, false)),
                    "timestamp-millis" => {
                        return Ok((
                            DataType::Timestamp(TimeUnit::Millisecond, None),
                            false,
                        ))
                    }
                    "timestamp-micros" => {
                        return Ok((
                            DataType::Timestamp(TimeUnit::Microsecond, None),
                            false,
                        ))
                    }
                    "decimal" => {
                        let precision = obj
                            .get("precision")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(38) as u8;
                        let scale = obj.get("scale").and_then(|v| v.as_i64()).unwrap_or(0) as i8;
                        return Ok((DataType::Decimal128(precision, scale), false));
                    }
                    _ => {}
                }
            }

            match obj.get("type").and_then(|v| v.as_str()) {
                Some("record") => {
                    let inner_fields = obj["fields"]
                        .as_array()
                        .ok_or_else(|| {
                            datafusion::common::DataFusionError::Plan(
                                "Avro record must have 'fields'".to_string(),
                            )
                        })?;
                    let fields: Vec<Field> = inner_fields
                        .iter()
                        .map(|f| {
                            let name = f["name"].as_str().unwrap_or("unknown").to_string();
                            let (dtype, nullable) = avro_type_to_arrow(&f["type"])?;
                            Ok(Field::new(name, dtype, nullable))
                        })
                        .collect::<Result<_>>()?;
                    Ok((DataType::Struct(fields.into()), false))
                }
                Some("array") => {
                    let (item_type, nullable) = avro_type_to_arrow(&obj["items"])?;
                    Ok((
                        DataType::List(Arc::new(Field::new("item", item_type, nullable))),
                        false,
                    ))
                }
                Some("map") => {
                    let (value_type, nullable) = avro_type_to_arrow(&obj["values"])?;
                    Ok((
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", value_type, nullable),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        false,
                    ))
                }
                Some("enum") => Ok((
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )),
                Some("fixed") => {
                    let size = obj
                        .get("size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as i32;
                    Ok((DataType::FixedSizeBinary(size), false))
                }
                Some(prim) => avro_type_to_arrow(&serde_json::Value::String(prim.to_string())),
                None => Err(datafusion::common::DataFusionError::Plan(
                    "Avro type object missing 'type' field".to_string(),
                )),
            }
        }
        _ => Err(datafusion::common::DataFusionError::Plan(format!(
            "unsupported Avro type value: {avro_type}"
        ))),
    }
}

fn json_schema_to_arrow(
    json_schema: &serde_json::Value,
) -> Result<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field};

    let properties = json_schema["properties"]
        .as_object()
        .ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "JSON Schema must have 'properties' for object type".to_string(),
            )
        })?;

    let required: Vec<String> = json_schema["required"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let fields: Vec<Field> = properties
        .iter()
        .map(|(name, prop)| {
            let dtype = json_type_to_arrow(prop)?;
            let nullable = !required.contains(name);
            Ok(Field::new(name, dtype, nullable))
        })
        .collect::<Result<_>>()?;

    Ok(arrow::datatypes::Schema::new(fields))
}

fn json_type_to_arrow(prop: &serde_json::Value) -> Result<arrow::datatypes::DataType> {
    use arrow::datatypes::{DataType, Field};

    match prop["type"].as_str() {
        Some("string") => Ok(DataType::Utf8),
        Some("integer") => Ok(DataType::Int64),
        Some("number") => Ok(DataType::Float64),
        Some("boolean") => Ok(DataType::Boolean),
        Some("object") => {
            let inner = json_schema_to_arrow(prop)?;
            Ok(DataType::Struct(inner.fields().clone()))
        }
        Some("array") => {
            let items = &prop["items"];
            let item_type = json_type_to_arrow(items)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", item_type, true,
            ))))
        }
        _ => Ok(DataType::Utf8),
    }
}

use std::sync::Arc;
