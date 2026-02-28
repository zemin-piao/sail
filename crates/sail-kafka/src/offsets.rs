use std::collections::HashMap;
use std::time::Duration;

use datafusion::common::{plan_err, DataFusionError, Result};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::Offset;
use rdkafka::TopicPartitionList;

use crate::error::KafkaError;
use crate::options::{KafkaReadOptions, OffsetSpec, TopicSelection};

#[derive(Debug, Clone)]
pub struct KafkaPartitionRange {
    pub topic: String,
    pub partition: i32,
    pub start_offset: i64,
    pub end_offset: i64,
}

pub fn resolve_offset_ranges(options: &KafkaReadOptions) -> Result<Vec<KafkaPartitionRange>> {
    let consumer: BaseConsumer = options
        .to_client_config()
        .create()
        .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;

    let topic_partitions = resolve_topic_partitions(&consumer, &options.topic_selection)?;

    let timeout = Duration::from_secs(30);
    let mut ranges = Vec::new();

    for (topic, partition) in &topic_partitions {
        let (low, high) = consumer
            .fetch_watermarks(topic, *partition, timeout)
            .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;

        let start = resolve_single_offset(
            &consumer,
            &options.starting_offsets,
            topic,
            *partition,
            low,
            high,
            true,
        )?;
        let end = resolve_single_offset(
            &consumer,
            &options.ending_offsets,
            topic,
            *partition,
            low,
            high,
            false,
        )?;

        if start < end {
            ranges.push(KafkaPartitionRange {
                topic: topic.clone(),
                partition: *partition,
                start_offset: start,
                end_offset: end,
            });
        }
    }

    if let Some(min_parts) = options.min_partitions {
        if min_parts > ranges.len() {
            ranges = split_ranges(ranges, min_parts);
        }
    }

    Ok(ranges)
}

fn resolve_topic_partitions(
    consumer: &BaseConsumer,
    selection: &TopicSelection,
) -> Result<Vec<(String, i32)>> {
    let timeout = Duration::from_secs(30);
    let metadata = consumer
        .fetch_metadata(None, timeout)
        .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;

    match selection {
        TopicSelection::Subscribe(topics) => {
            let mut result = Vec::new();
            for topic_name in topics {
                let topic_meta = metadata
                    .topics()
                    .iter()
                    .find(|t| t.name() == topic_name)
                    .ok_or_else(|| {
                        DataFusionError::from(KafkaError::Config(format!(
                            "topic not found: {topic_name}"
                        )))
                    })?;
                for p in topic_meta.partitions() {
                    result.push((topic_name.clone(), p.id()));
                }
            }
            Ok(result)
        }
        TopicSelection::SubscribePattern(pattern) => {
            let re = regex::Regex::new(pattern).map_err(|e| {
                DataFusionError::from(KafkaError::Config(format!(
                    "invalid subscribePattern regex: {e}"
                )))
            })?;
            let mut result = Vec::new();
            for topic in metadata.topics() {
                if re.is_match(topic.name()) {
                    for p in topic.partitions() {
                        result.push((topic.name().to_string(), p.id()));
                    }
                }
            }
            Ok(result)
        }
        TopicSelection::Assign(assignments) => {
            let mut result = Vec::new();
            for (topic, partitions) in assignments {
                for p in partitions {
                    result.push((topic.clone(), *p));
                }
            }
            Ok(result)
        }
    }
}

fn resolve_single_offset(
    consumer: &BaseConsumer,
    spec: &OffsetSpec,
    topic: &str,
    partition: i32,
    low_watermark: i64,
    high_watermark: i64,
    is_starting: bool,
) -> Result<i64> {
    match spec {
        OffsetSpec::Earliest => Ok(low_watermark),
        OffsetSpec::Latest => Ok(high_watermark),
        OffsetSpec::PerPartition(map) => {
            if let Some(topic_offsets) = map.get(topic) {
                if let Some(&offset) = topic_offsets.get(&partition) {
                    if offset == -2 {
                        return Ok(low_watermark);
                    }
                    if offset == -1 {
                        return Ok(high_watermark);
                    }
                    return Ok(offset);
                }
            }
            if is_starting {
                Ok(low_watermark)
            } else {
                Ok(high_watermark)
            }
        }
        OffsetSpec::GlobalTimestamp(ms) => {
            resolve_offset_by_timestamp(consumer, topic, partition, *ms, is_starting)
        }
        OffsetSpec::ByTimestamp(map) => {
            if let Some(topic_ts) = map.get(topic) {
                if let Some(&ms) = topic_ts.get(&partition) {
                    return resolve_offset_by_timestamp(
                        consumer, topic, partition, ms, is_starting,
                    );
                }
            }
            if is_starting {
                Ok(low_watermark)
            } else {
                Ok(high_watermark)
            }
        }
    }
}

fn resolve_offset_by_timestamp(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    timestamp_ms: i64,
    is_starting: bool,
) -> Result<i64> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(timestamp_ms))
        .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;

    let result = consumer
        .offsets_for_times(tpl, Duration::from_secs(30))
        .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;

    if let Some(elem) = result.elements().first() {
        match elem.offset() {
            Offset::Offset(o) => Ok(o),
            Offset::End => {
                let (_, high) = consumer
                    .fetch_watermarks(topic, partition, Duration::from_secs(10))
                    .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;
                Ok(high)
            }
            _ => {
                if is_starting {
                    Err(DataFusionError::from(KafkaError::Config(format!(
                        "no offset found for timestamp {timestamp_ms} on {topic}-{partition}"
                    ))))
                } else {
                    let (_, high) = consumer
                        .fetch_watermarks(topic, partition, Duration::from_secs(10))
                        .map_err(|e| DataFusionError::from(KafkaError::Kafka(e)))?;
                    Ok(high)
                }
            }
        }
    } else {
        Err(DataFusionError::from(KafkaError::Config(format!(
            "failed to resolve offset for timestamp {timestamp_ms} on {topic}-{partition}"
        ))))
    }
}

fn split_ranges(
    ranges: Vec<KafkaPartitionRange>,
    min_partitions: usize,
) -> Vec<KafkaPartitionRange> {
    let total_size: i64 = ranges.iter().map(|r| r.end_offset - r.start_offset).sum();
    if total_size == 0 {
        return ranges;
    }

    let mut result = Vec::new();
    for range in &ranges {
        let size = range.end_offset - range.start_offset;
        let parts =
            ((size as f64 / total_size as f64) * min_partitions as f64).round().max(1.0) as usize;

        if parts <= 1 {
            result.push(range.clone());
        } else {
            let step = size / parts as i64;
            let mut start = range.start_offset;
            for i in 0..parts {
                let end = if i == parts - 1 {
                    range.end_offset
                } else {
                    start + step
                };
                if start < end {
                    result.push(KafkaPartitionRange {
                        topic: range.topic.clone(),
                        partition: range.partition,
                        start_offset: start,
                        end_offset: end,
                    });
                }
                start = end;
            }
        }
    }
    result
}
