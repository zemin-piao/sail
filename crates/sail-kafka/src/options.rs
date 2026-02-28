use std::collections::HashMap;

use datafusion::common::{plan_err, DataFusionError, Result};
use rdkafka::ClientConfig;

use crate::error::KafkaError;

const BLOCKED_KAFKA_CONFIGS: &[&str] = &[
    "auto.offset.reset",
    "key.deserializer",
    "value.deserializer",
    "enable.auto.commit",
    "interceptor.classes",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicSelection {
    Subscribe(Vec<String>),
    SubscribePattern(String),
    Assign(HashMap<String, Vec<i32>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetSpec {
    Earliest,
    Latest,
    PerPartition(HashMap<String, HashMap<i32, i64>>),
    ByTimestamp(HashMap<String, HashMap<i32, i64>>),
    GlobalTimestamp(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueFormat {
    Avro,
    Json,
    Raw,
}

#[derive(Debug, Clone)]
pub struct KafkaReadOptions {
    pub bootstrap_servers: String,
    pub topic_selection: TopicSelection,
    pub starting_offsets: OffsetSpec,
    pub ending_offsets: OffsetSpec,
    pub fail_on_data_loss: bool,
    pub include_headers: bool,
    pub min_partitions: Option<usize>,
    pub group_id_prefix: String,
    pub poll_timeout_ms: u64,
    pub fetch_offset_num_retries: u32,
    pub fetch_offset_retry_interval_ms: u64,
    // LakeSail extensions (not in Spark)
    pub value_format: Option<ValueFormat>,
    pub schema_registry_url: Option<String>,
    pub value_schema_subject: Option<String>,
    pub kafka_properties: HashMap<String, String>,
}

impl KafkaReadOptions {
    pub fn from_options(options: &[HashMap<String, String>]) -> Result<Self> {
        let merged = Self::merge_options_case_insensitive(options);

        let bootstrap_servers = merged
            .get("kafka.bootstrap.servers")
            .cloned()
            .ok_or_else(|| {
                DataFusionError::from(KafkaError::Config(
                    "kafka.bootstrap.servers is required".to_string(),
                ))
            })?;

        Self::validate_blocked_configs(&merged)?;

        let topic_selection = Self::parse_topic_selection(&merged)?;

        // Offset priority: globalTimestamp > byTimestamp > offsets
        let starting_offsets = Self::resolve_starting_offsets(&merged)?;
        let ending_offsets = Self::resolve_ending_offsets(&merged)?;

        let fail_on_data_loss = merged
            .get("failondataloss")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        let include_headers = merged
            .get("includeheaders")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let min_partitions = merged
            .get("minpartitions")
            .map(|v| {
                v.parse::<usize>().map_err(|_| {
                    DataFusionError::from(KafkaError::Config(format!(
                        "invalid minPartitions value: {v}"
                    )))
                })
            })
            .transpose()?;

        let group_id_prefix = merged
            .get("groupidprefix")
            .cloned()
            .unwrap_or_else(|| "spark-kafka-relation".to_string());

        let poll_timeout_ms = merged
            .get("kafkaconsumer.polltimeoutms")
            .map(|v| {
                v.parse::<u64>().map_err(|_| {
                    DataFusionError::from(KafkaError::Config(format!(
                        "invalid kafkaConsumer.pollTimeoutMs: {v}"
                    )))
                })
            })
            .transpose()?
            .unwrap_or(120_000);

        let fetch_offset_num_retries = merged
            .get("fetchoffset.numretries")
            .map(|v| {
                v.parse::<u32>().map_err(|_| {
                    DataFusionError::from(KafkaError::Config(format!(
                        "invalid fetchOffset.numRetries: {v}"
                    )))
                })
            })
            .transpose()?
            .unwrap_or(3);

        let fetch_offset_retry_interval_ms = merged
            .get("fetchoffset.retryintervalms")
            .map(|v| {
                v.parse::<u64>().map_err(|_| {
                    DataFusionError::from(KafkaError::Config(format!(
                        "invalid fetchOffset.retryIntervalMs: {v}"
                    )))
                })
            })
            .transpose()?
            .unwrap_or(1000);

        let value_format = merged
            .get("value.format")
            .map(|v| match v.as_str() {
                "avro" => Ok(ValueFormat::Avro),
                "json" => Ok(ValueFormat::Json),
                "raw" => Ok(ValueFormat::Raw),
                other => plan_err!(
                    "unsupported value.format: {other}. Supported: avro, json, raw"
                ),
            })
            .transpose()?;

        let schema_registry_url = merged.get("schema.registry.url").cloned();
        let value_schema_subject = merged.get("value.schema.subject").cloned();

        // Warn about stream-only options
        for opt in &["maxoffsetspertrigger", "minoffsetspertrigger", "maxtriggerdelay"] {
            if merged.contains_key(*opt) {
                log::warn!("Kafka option '{opt}' is not supported in batch mode and will be ignored");
            }
        }

        let kafka_properties: HashMap<String, String> = merged
            .iter()
            .filter(|(k, _)| k.starts_with("kafka.") && *k != "kafka.bootstrap.servers")
            .map(|(k, v)| {
                (
                    k.strip_prefix("kafka.").unwrap_or(k).to_string(),
                    v.clone(),
                )
            })
            .collect();

        Ok(Self {
            bootstrap_servers,
            topic_selection,
            starting_offsets,
            ending_offsets,
            fail_on_data_loss,
            include_headers,
            min_partitions,
            group_id_prefix,
            poll_timeout_ms,
            fetch_offset_num_retries,
            fetch_offset_retry_interval_ms,
            value_format,
            schema_registry_url,
            value_schema_subject,
            kafka_properties,
        })
    }

    /// Merge options using case-insensitive keys (Spark uses CaseInsensitiveMap)
    fn merge_options_case_insensitive(
        options: &[HashMap<String, String>],
    ) -> HashMap<String, String> {
        let mut merged: HashMap<String, String> = HashMap::new();
        for opt in options {
            for (k, v) in opt {
                merged.insert(k.to_lowercase(), v.clone());
            }
        }
        merged
    }

    fn validate_blocked_configs(merged: &HashMap<String, String>) -> Result<()> {
        for blocked in BLOCKED_KAFKA_CONFIGS {
            let key = format!("kafka.{blocked}");
            if merged.contains_key(&key) {
                return plan_err!(
                    "Kafka option '{blocked}' is not supported. {}",
                    match *blocked {
                        "key.deserializer" =>
                            "Keys are deserialized as byte arrays with ByteArrayDeserializer.",
                        "value.deserializer" =>
                            "Values are deserialized as byte arrays with ByteArrayDeserializer.",
                        "auto.offset.reset" =>
                            "Set startingOffsets/endingOffsets to control offsets.",
                        "enable.auto.commit" => "Offsets are not committed in batch mode.",
                        "interceptor.classes" =>
                            "Interceptors are not supported.",
                        _ => "",
                    }
                );
            }
        }
        Ok(())
    }

    fn parse_topic_selection(opts: &HashMap<String, String>) -> Result<TopicSelection> {
        let subscribe = opts.get("subscribe");
        let pattern = opts.get("subscribepattern");
        let assign = opts.get("assign");

        let count =
            subscribe.is_some() as u8 + pattern.is_some() as u8 + assign.is_some() as u8;
        if count == 0 {
            return plan_err!(
                "one of 'subscribe', 'subscribePattern', or 'assign' is required"
            );
        }
        if count > 1 {
            return plan_err!(
                "'subscribe', 'subscribePattern', and 'assign' are mutually exclusive"
            );
        }

        if let Some(s) = subscribe {
            let topics: Vec<String> = s.split(',').map(|t| t.trim().to_string()).collect();
            return Ok(TopicSelection::Subscribe(topics));
        }
        if let Some(p) = pattern {
            return Ok(TopicSelection::SubscribePattern(p.clone()));
        }
        if let Some(a) = assign {
            let parsed: HashMap<String, Vec<i32>> = serde_json::from_str(a).map_err(|e| {
                DataFusionError::from(KafkaError::Config(format!(
                    "invalid 'assign' JSON: {e}"
                )))
            })?;
            return Ok(TopicSelection::Assign(parsed));
        }

        plan_err!("one of 'subscribe', 'subscribePattern', or 'assign' is required")
    }

    /// Resolve starting offsets with Spark priority: startingTimestamp > startingOffsetsByTimestamp > startingOffsets
    fn resolve_starting_offsets(merged: &HashMap<String, String>) -> Result<OffsetSpec> {
        if let Some(ts) = merged.get("startingtimestamp") {
            let ms: i64 = ts.parse().map_err(|_| {
                DataFusionError::from(KafkaError::Config(format!(
                    "invalid startingTimestamp: {ts}"
                )))
            })?;
            return Ok(OffsetSpec::GlobalTimestamp(ms));
        }
        if let Some(json) = merged.get("startingoffsetsbytimestamp") {
            let parsed: HashMap<String, HashMap<String, i64>> =
                serde_json::from_str(json).map_err(|e| {
                    DataFusionError::from(KafkaError::Config(format!(
                        "invalid startingOffsetsByTimestamp JSON: {e}"
                    )))
                })?;
            let converted = Self::convert_partition_map(parsed)?;
            return Ok(OffsetSpec::ByTimestamp(converted));
        }
        Self::parse_offsets(merged.get("startingoffsets"), true)
    }

    /// Resolve ending offsets with Spark priority: endingTimestamp > endingOffsetsByTimestamp > endingOffsets
    fn resolve_ending_offsets(merged: &HashMap<String, String>) -> Result<OffsetSpec> {
        if let Some(ts) = merged.get("endingtimestamp") {
            let ms: i64 = ts.parse().map_err(|_| {
                DataFusionError::from(KafkaError::Config(format!(
                    "invalid endingTimestamp: {ts}"
                )))
            })?;
            return Ok(OffsetSpec::GlobalTimestamp(ms));
        }
        if let Some(json) = merged.get("endingoffsetsbytimestamp") {
            let parsed: HashMap<String, HashMap<String, i64>> =
                serde_json::from_str(json).map_err(|e| {
                    DataFusionError::from(KafkaError::Config(format!(
                        "invalid endingOffsetsByTimestamp JSON: {e}"
                    )))
                })?;
            let converted = Self::convert_partition_map(parsed)?;
            return Ok(OffsetSpec::ByTimestamp(converted));
        }
        Self::parse_offsets(merged.get("endingoffsets"), false)
    }

    fn convert_partition_map(
        parsed: HashMap<String, HashMap<String, i64>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>> {
        parsed
            .into_iter()
            .map(|(topic, partitions)| {
                let parts: HashMap<i32, i64> = partitions
                    .into_iter()
                    .map(|(p, o)| {
                        let partition: i32 = p.parse().map_err(|_| {
                            DataFusionError::from(KafkaError::Config(format!(
                                "invalid partition number: {p}"
                            )))
                        })?;
                        Ok((partition, o))
                    })
                    .collect::<Result<_>>()?;
                Ok((topic, parts))
            })
            .collect::<Result<_>>()
    }

    fn parse_offsets(value: Option<&String>, is_starting: bool) -> Result<OffsetSpec> {
        match value {
            None => {
                if is_starting {
                    Ok(OffsetSpec::Earliest)
                } else {
                    Ok(OffsetSpec::Latest)
                }
            }
            Some(v) if v.eq_ignore_ascii_case("earliest") => {
                if !is_starting {
                    return plan_err!(
                        "'earliest' is not allowed for endingOffsets in batch mode"
                    );
                }
                Ok(OffsetSpec::Earliest)
            }
            Some(v) if v.eq_ignore_ascii_case("latest") => {
                if is_starting {
                    return plan_err!(
                        "'latest' is not allowed for startingOffsets in batch mode"
                    );
                }
                Ok(OffsetSpec::Latest)
            }
            Some(v) => {
                let parsed: HashMap<String, HashMap<String, i64>> =
                    serde_json::from_str(v).map_err(|e| {
                        DataFusionError::from(KafkaError::Config(format!(
                            "invalid offsets JSON: {e}"
                        )))
                    })?;
                let converted = Self::convert_partition_map(parsed)?;
                Ok(OffsetSpec::PerPartition(converted))
            }
        }
    }

    pub fn to_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &self.bootstrap_servers);
        config.set("enable.auto.commit", "false");
        config.set(
            "key.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        );
        config.set(
            "value.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        );
        // Auto-generate group.id like Spark: {groupIdPrefix}-{uuid}
        let group_id = format!("{}-{}", self.group_id_prefix, uuid_v4());
        config.set("group.id", &group_id);
        for (k, v) in &self.kafka_properties {
            config.set(k, v);
        }
        config
    }
}

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", ts)
}
