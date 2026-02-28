use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    ArrayRef, BinaryBuilder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::Schema as ArrowSchema;
use arrow_json::ReaderBuilder as JsonReaderBuilder;
use datafusion::common::{DataFusionError, Result};
use futures::stream::{self, Stream};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::TopicPartitionList;

use crate::encoding::ValueEncoding;
use crate::offsets::KafkaPartitionRange;
use crate::options::KafkaReadOptions;

pub fn kafka_partition_stream(
    options: KafkaReadOptions,
    range: KafkaPartitionRange,
    encoding: ValueEncoding,
    schema: Arc<ArrowSchema>,
    batch_size: usize,
) -> impl Stream<Item = Result<RecordBatch>> {
    stream::try_unfold(
        StreamState::new(options, range, encoding, schema, batch_size),
        |mut state| async move { state.next_batch().await },
    )
}

struct KafkaMessage {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_us: i64,
    timestamp_type: i32,
}

struct StreamState {
    consumer: Option<BaseConsumer>,
    options: KafkaReadOptions,
    range: KafkaPartitionRange,
    encoding: ValueEncoding,
    schema: Arc<ArrowSchema>,
    batch_size: usize,
    done: bool,
}

impl StreamState {
    fn new(
        options: KafkaReadOptions,
        range: KafkaPartitionRange,
        encoding: ValueEncoding,
        schema: Arc<ArrowSchema>,
        batch_size: usize,
    ) -> Self {
        Self {
            consumer: None,
            options,
            range,
            encoding,
            schema,
            batch_size,
            done: false,
        }
    }

    fn init_consumer(&mut self) -> Result<()> {
        let consumer: BaseConsumer = self
            .options
            .to_client_config()
            .create()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            &self.range.topic,
            self.range.partition,
            rdkafka::Offset::Offset(self.range.start_offset),
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        consumer
            .assign(&tpl)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // failOnDataLoss check: verify the start offset is reachable
        let timeout = Duration::from_secs(10);
        let (low, _high) = consumer
            .fetch_watermarks(&self.range.topic, self.range.partition, timeout)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if self.range.start_offset < low {
            if self.options.fail_on_data_loss {
                return Err(DataFusionError::External(Box::new(
                    crate::error::KafkaError::Config(format!(
                        "data loss detected: requested start offset {} for {}-{} but earliest available is {}. \
                         Set failOnDataLoss=false to skip missing data.",
                        self.range.start_offset, self.range.topic, self.range.partition, low
                    )),
                )));
            }
            log::warn!(
                "Data loss: start offset {} for {}-{} unavailable (earliest={}), skipping to {}",
                self.range.start_offset,
                self.range.topic,
                self.range.partition,
                low,
                low
            );
            // Re-assign with the corrected offset
            let mut tpl2 = TopicPartitionList::new();
            tpl2.add_partition_offset(
                &self.range.topic,
                self.range.partition,
                rdkafka::Offset::Offset(low),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            consumer
                .assign(&tpl2)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        }

        self.consumer = Some(consumer);
        Ok(())
    }

    fn poll_messages(&mut self, consumer: &BaseConsumer) -> Result<Vec<KafkaMessage>> {
        let mut messages = Vec::new();
        let poll_timeout = Duration::from_millis(500);
        let mut empty_polls = 0;

        while messages.len() < self.batch_size {
            match consumer.poll(poll_timeout) {
                Some(Ok(msg)) => {
                    empty_polls = 0;
                    let msg_offset = msg.offset();
                    if msg_offset >= self.range.end_offset {
                        self.done = true;
                        break;
                    }
                    messages.push(KafkaMessage {
                        key: msg.key().map(|k| k.to_vec()),
                        value: msg.payload().map(|v| v.to_vec()),
                        topic: msg.topic().to_string(),
                        partition: msg.partition(),
                        offset: msg_offset,
                        timestamp_us: msg.timestamp().to_millis().unwrap_or(0) * 1000,
                        timestamp_type: match msg.timestamp() {
                            rdkafka::Timestamp::NotAvailable => -1,
                            rdkafka::Timestamp::CreateTime(_) => 0,
                            rdkafka::Timestamp::LogAppendTime(_) => 1,
                        },
                    });
                }
                Some(Err(e)) => {
                    return Err(DataFusionError::External(Box::new(e)));
                }
                None => {
                    empty_polls += 1;
                    if empty_polls >= 3 || !messages.is_empty() {
                        break;
                    }
                }
            }
        }
        Ok(messages)
    }

    async fn next_batch(&mut self) -> Result<Option<(RecordBatch, Self)>> {
        if self.done {
            return Ok(None);
        }

        if self.consumer.is_none() {
            self.init_consumer()?;
        }

        let consumer = self.consumer.as_ref().ok_or_else(|| {
            DataFusionError::Internal("consumer not initialized".to_string())
        })?;

        let messages = self.poll_messages(consumer)?;
        if messages.is_empty() {
            self.done = true;
            return Ok(None);
        }

        let batch = match &self.encoding {
            ValueEncoding::Raw => self.build_raw_batch(&messages)?,
            ValueEncoding::Avro { reader_schema_json } => {
                self.build_avro_batch(&messages, reader_schema_json)?
            }
            ValueEncoding::Json { schema } => self.build_json_batch(&messages, schema)?,
        };

        let next_state = StreamState {
            consumer: self.consumer.take(),
            options: self.options.clone(),
            range: self.range.clone(),
            encoding: self.encoding.clone(),
            schema: self.schema.clone(),
            batch_size: self.batch_size,
            done: self.done,
        };

        Ok(Some((batch, next_state)))
    }

    fn build_raw_batch(&self, messages: &[KafkaMessage]) -> Result<RecordBatch> {
        let mut key_builder = BinaryBuilder::new();
        let mut value_builder = BinaryBuilder::new();
        let mut topic_builder = StringBuilder::new();
        let mut partition_builder = Int32Builder::new();
        let mut offset_builder = Int64Builder::new();
        let mut timestamp_builder = TimestampMicrosecondBuilder::new();
        let mut timestamp_type_builder = Int32Builder::new();

        for msg in messages {
            match &msg.key {
                Some(k) => key_builder.append_value(k),
                None => key_builder.append_null(),
            }
            match &msg.value {
                Some(v) => value_builder.append_value(v),
                None => value_builder.append_null(),
            }
            topic_builder.append_value(&msg.topic);
            partition_builder.append_value(msg.partition);
            offset_builder.append_value(msg.offset);
            timestamp_builder.append_value(msg.timestamp_us);
            timestamp_type_builder.append_value(msg.timestamp_type);
        }

        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
                Arc::new(topic_builder.finish()),
                Arc::new(partition_builder.finish()),
                Arc::new(offset_builder.finish()),
                Arc::new(timestamp_builder.finish()),
                Arc::new(timestamp_type_builder.finish()),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(e, None))
    }

    fn build_avro_batch(
        &self,
        messages: &[KafkaMessage],
        _reader_schema_json: &str,
    ) -> Result<RecordBatch> {
        // Accumulate Avro payloads (strip 5-byte Confluent wire format prefix)
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(messages.len());
        let mut metadata = MetadataAccumulator::new(messages.len());

        for msg in messages {
            metadata.push(msg);
            if let Some(value) = &msg.value {
                if value.len() >= 5 && value[0] == 0x00 {
                    // Confluent wire format: magic byte (0x00) + 4-byte schema ID + payload
                    payloads.push(value[5..].to_vec());
                } else {
                    payloads.push(value.clone());
                }
            } else {
                payloads.push(Vec::new());
            }
        }

        // Build OCF-style framed buffer for arrow-avro Decoder
        // For now, use the raw binary fallback and let arrow-avro handle via its Decoder
        // TODO: wire up arrow_avro::reader::ReaderBuilder with SchemaStore for full
        //       Confluent wire format support once arrow-avro streaming Decoder API is stable
        //
        // Fallback: decode via apache-avro per-record and build Arrow arrays manually
        // For the initial implementation, return raw mode with metadata
        self.build_raw_batch(messages)
    }

    fn build_json_batch(
        &self,
        messages: &[KafkaMessage],
        value_schema: &ArrowSchema,
    ) -> Result<RecordBatch> {
        // Accumulate JSON payloads into a newline-delimited buffer
        let mut json_buf = Vec::new();
        let mut metadata = MetadataAccumulator::new(messages.len());

        for msg in messages {
            metadata.push(msg);
            if let Some(value) = &msg.value {
                json_buf.extend_from_slice(value);
                json_buf.push(b'\n');
            }
        }

        // Parse JSON into Arrow RecordBatch using arrow-json
        let cursor = Cursor::new(json_buf);
        let json_reader = JsonReaderBuilder::new(Arc::new(value_schema.clone()))
            .build(cursor)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        let mut value_batches: Vec<RecordBatch> = Vec::new();
        for batch_result in json_reader {
            let batch = batch_result.map_err(|e| DataFusionError::ArrowError(e, None))?;
            value_batches.push(batch);
        }

        if value_batches.is_empty() {
            return self.build_raw_batch(messages);
        }

        // Concatenate value batches and append metadata columns
        let value_batch = arrow::compute::concat_batches(
            &Arc::new(value_schema.clone()),
            value_batches.iter(),
        )
        .map_err(|e| DataFusionError::ArrowError(e, None))?;

        let mut columns: Vec<ArrayRef> = value_batch.columns().to_vec();
        columns.extend(metadata.finish());

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))
    }
}

struct MetadataAccumulator {
    key_builder: BinaryBuilder,
    topic_builder: StringBuilder,
    partition_builder: Int32Builder,
    offset_builder: Int64Builder,
    timestamp_builder: TimestampMicrosecondBuilder,
}

impl MetadataAccumulator {
    fn new(capacity: usize) -> Self {
        Self {
            key_builder: BinaryBuilder::with_capacity(capacity, 0),
            topic_builder: StringBuilder::with_capacity(capacity, 0),
            partition_builder: Int32Builder::with_capacity(capacity),
            offset_builder: Int64Builder::with_capacity(capacity),
            timestamp_builder: TimestampMicrosecondBuilder::with_capacity(capacity),
        }
    }

    fn push(&mut self, msg: &KafkaMessage) {
        match &msg.key {
            Some(k) => self.key_builder.append_value(k),
            None => self.key_builder.append_null(),
        }
        self.topic_builder.append_value(&msg.topic);
        self.partition_builder.append_value(msg.partition);
        self.offset_builder.append_value(msg.offset);
        self.timestamp_builder.append_value(msg.timestamp_us);
    }

    fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.key_builder.finish()),
            Arc::new(self.topic_builder.finish()),
            Arc::new(self.partition_builder.finish()),
            Arc::new(self.offset_builder.finish()),
            Arc::new(self.timestamp_builder.finish()),
        ]
    }
}
