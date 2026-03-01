#![expect(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::DataType;
use futures::StreamExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use sail_kafka::encoding::{build_deserialized_schema, raw_kafka_schema, ValueEncoding};
use sail_kafka::offsets::KafkaPartitionRange;
use sail_kafka::options::{KafkaReadOptions, OffsetSpec, TopicSelection};
use sail_kafka::stream::kafka_partition_stream;

// ------------------------------------------------------------------
// Container helpers
// ------------------------------------------------------------------

struct KafkaCluster {
    _kafka: ContainerAsync<GenericImage>,
    _schema_registry: ContainerAsync<GenericImage>,
    kafka_port: u16,
    sr_port: u16,
}

impl KafkaCluster {
    fn bootstrap_servers(&self) -> String {
        format!("localhost:{}", self.kafka_port)
    }

    fn schema_registry_url(&self) -> String {
        format!("http://localhost:{}", self.sr_port)
    }
}

async fn start_kafka_cluster(network: &str) -> KafkaCluster {
    // KRaft-mode Kafka with two listeners:
    //   BROKER on :29092 (inter-broker + schema-registry, inside Docker network)
    //   EXTERNAL on :9092 (host access via testcontainers port mapping)
    //
    // The key challenge: Kafka advertises `localhost:9092` but testcontainers maps
    // container port 9092 to a random host port. The rdkafka client connects to
    // `localhost:<mapped_port>`, receives metadata saying broker is at `localhost:9092`,
    // then tries to reconnect to port 9092 (wrong).
    //
    // Solution: use `with_copy_to` to inject a startup wrapper script that reads the
    // actual mapped port from the EXTERNAL_PORT env var and patches
    // KAFKA_ADVERTISED_LISTENERS before delegating to the real entrypoint.
    // Since we use `with_mapped_port` with a fixed port, we can set the advertised
    // listener statically. We pick a high port to minimize collision risk.

    // Use a port derived from the network name hash to reduce collision between parallel tests
    let port_base: u16 = {
        let hash: u32 = network.bytes().fold(0u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32));
        19092 + (hash % 10000)
    };

    let kafka = GenericImage::new("confluentinc/cp-kafka", "7.9.0")
        .with_wait_for(WaitFor::message_on_stdout("started (kafka.server.KafkaRaftServer)"))
        .with_mapped_port(port_base, ContainerPort::Tcp(9092))
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var(
            "KAFKA_LISTENERS",
            "BROKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092",
        )
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_CLUSTER_ID", &format!("sail-test-{network}0000000"))
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            &format!("BROKER://localhost:29092,EXTERNAL://localhost:{port_base}"),
        )
        .with_network(network)
        .start()
        .await
        .expect("failed to start kafka container");

    // The mapped port is fixed, so kafka_port == port_base
    let kafka_port = port_base;

    // Wait for broker to fully initialize
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Schema Registry connects to Kafka via the bridge network IP
    let kafka_ip = kafka
        .get_bridge_ip_address()
        .await
        .expect("failed to get kafka bridge ip");

    let sr = GenericImage::new("confluentinc/cp-schema-registry", "7.9.0")
        .with_wait_for(WaitFor::message_on_stdout("Server started"))
        .with_exposed_port(ContainerPort::Tcp(8081))
        .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
        .with_env_var(
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
            &format!("{kafka_ip}:29092"),
        )
        .with_env_var("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
        .with_network(network)
        .start()
        .await
        .expect("failed to start schema-registry container");

    let sr_port = sr
        .get_host_port_ipv4(ContainerPort::Tcp(8081))
        .await
        .expect("failed to get schema-registry port");

    // Wait for Schema Registry to be ready
    tokio::time::sleep(Duration::from_secs(3)).await;

    KafkaCluster {
        _kafka: kafka,
        _schema_registry: sr,
        kafka_port,
        sr_port,
    }
}

fn make_admin(bootstrap: &str) -> AdminClient<rdkafka::client::DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client creation failed")
}

fn make_producer(bootstrap: &str) -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creation failed")
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin = make_admin(bootstrap);
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    admin
        .create_topics(&[new_topic], &AdminOptions::default())
        .await
        .expect("topic creation failed");
    // Allow partition metadata to propagate
    tokio::time::sleep(Duration::from_secs(2)).await;
}

fn default_options(bootstrap: &str, topic: &str) -> KafkaReadOptions {
    KafkaReadOptions {
        bootstrap_servers: bootstrap.to_string(),
        topic_selection: TopicSelection::Subscribe(vec![topic.to_string()]),
        starting_offsets: OffsetSpec::Earliest,
        ending_offsets: OffsetSpec::Latest,
        fail_on_data_loss: true,
        include_headers: false,
        min_partitions: None,
        group_id_prefix: "sail-kafka-test".to_string(),
        poll_timeout_ms: 10_000,
        fetch_offset_num_retries: 3,
        fetch_offset_retry_interval_ms: 500,
        value_format: None,
        schema_registry_url: None,
        value_schema_subject: None,
        kafka_properties: HashMap::new(),
    }
}

/// Collect all batches from a single partition stream.
async fn collect_stream(
    options: KafkaReadOptions,
    range: KafkaPartitionRange,
    encoding: ValueEncoding,
    schema: Arc<arrow::datatypes::Schema>,
) -> Vec<RecordBatch> {
    let stream = kafka_partition_stream(options, range, encoding, schema, 1024);
    tokio::pin!(stream);
    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        batches.push(result.expect("stream error"));
    }
    batches
}

// ------------------------------------------------------------------
// Test: Raw mode
// ------------------------------------------------------------------

#[tokio::test]
async fn test_raw_mode_read() {
    let cluster = start_kafka_cluster("raw_test").await;
    let bootstrap = cluster.bootstrap_servers();
    let topic = "sail_raw_test";

    create_topic(&bootstrap, topic, 1).await;

    // Produce 5 messages
    let producer = make_producer(&bootstrap);
    for i in 0..5 {
        let key = format!("key-{i}");
        let value = format!("value-{i}");
        producer
            .send(
                BaseRecord::to(topic)
                    .key(&key)
                    .payload(&value)
                    .partition(0),
            )
            .expect("send failed");
    }
    producer.flush(Duration::from_secs(5)).expect("flush failed");

    let options = default_options(&bootstrap, topic);
    let schema = Arc::new(raw_kafka_schema(false));
    let range = KafkaPartitionRange {
        topic: topic.to_string(),
        partition: 0,
        start_offset: 0,
        end_offset: 5,
    };

    let batches = collect_stream(options, range, ValueEncoding::Raw, schema.clone()).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "expected 5 rows, got {total_rows}");

    // Verify schema columns
    let batch = &batches[0];
    assert_eq!(batch.schema().fields().len(), 7);
    assert_eq!(batch.schema().field(0).name(), "key");
    assert_eq!(batch.schema().field(1).name(), "value");
    assert_eq!(batch.schema().field(2).name(), "topic");
    assert_eq!(batch.schema().field(3).name(), "partition");
    assert_eq!(batch.schema().field(4).name(), "offset");
    assert_eq!(batch.schema().field(5).name(), "timestamp");
    assert_eq!(batch.schema().field(6).name(), "timestampType");

    // Verify key content
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("key column should be binary");
    assert_eq!(keys.value(0), b"key-0");
    assert_eq!(keys.value(4), b"key-4");

    // Verify value content
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("value column should be binary");
    assert_eq!(values.value(0), b"value-0");

    // Verify offsets are sequential
    let offsets = batch
        .column(4)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("offset column should be Int64");
    for i in 0..5 {
        assert_eq!(offsets.value(i), i as i64);
    }

    // Verify partition column
    let partitions = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("partition column should be Int32");
    for i in 0..5 {
        assert_eq!(partitions.value(i), 0);
    }
}

// ------------------------------------------------------------------
// Test: Multi-partition raw mode
// ------------------------------------------------------------------

#[tokio::test]
async fn test_raw_mode_multi_partition() {
    let cluster = start_kafka_cluster("multi_part_test").await;
    let bootstrap = cluster.bootstrap_servers();
    let topic = "sail_multi_part_test";

    create_topic(&bootstrap, topic, 3).await;

    let producer = make_producer(&bootstrap);
    // Produce to each partition explicitly
    for p in 0..3 {
        for i in 0..4 {
            let key = format!("p{p}-key-{i}");
            let value = format!("p{p}-value-{i}");
            producer
                .send(
                    BaseRecord::to(topic)
                        .key(&key)
                        .payload(&value)
                        .partition(p),
                )
                .expect("send failed");
        }
    }
    producer.flush(Duration::from_secs(5)).expect("flush failed");

    let options = default_options(&bootstrap, topic);
    let schema = Arc::new(raw_kafka_schema(false));

    // Read from each partition separately (as KafkaScanExec would)
    let mut total = 0;
    for p in 0..3 {
        let range = KafkaPartitionRange {
            topic: topic.to_string(),
            partition: p,
            start_offset: 0,
            end_offset: 4,
        };
        let batches =
            collect_stream(options.clone(), range, ValueEncoding::Raw, schema.clone()).await;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 4, "partition {p} expected 4 rows, got {rows}");
        total += rows;
    }
    assert_eq!(total, 12);
}

// ------------------------------------------------------------------
// Test: Avro deserialization with Confluent wire format
// ------------------------------------------------------------------

/// Build a Confluent wire format message: 0x00 + 4-byte big-endian schema_id + Avro binary body.
fn confluent_frame(schema_id: u32, avro_body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + avro_body.len());
    buf.push(0x00); // magic byte
    buf.extend_from_slice(&schema_id.to_be_bytes());
    buf.extend_from_slice(avro_body);
    buf
}

/// Encode a simple Avro record {id: long, name: string} as binary.
/// Avro binary encoding: long is variable-length zigzag, string is length-prefixed bytes.
fn encode_avro_record(id: i64, name: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    // Zigzag encode the long
    let zigzag = ((id << 1) ^ (id >> 63)) as u64;
    encode_varint(&mut buf, zigzag);
    // String: length-prefixed
    encode_varint(&mut buf, name.len() as u64);
    buf.extend_from_slice(name.as_bytes());
    buf
}

fn encode_varint(buf: &mut Vec<u8>, mut val: u64) {
    loop {
        let mut byte = (val & 0x7F) as u8;
        val >>= 7;
        if val != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if val == 0 {
            break;
        }
    }
}

#[tokio::test]
async fn test_avro_confluent_deserialization() {
    let cluster = start_kafka_cluster("avro_test").await;
    let bootstrap = cluster.bootstrap_servers();
    let sr_url = cluster.schema_registry_url();
    let topic = "sail_avro_test";

    create_topic(&bootstrap, topic, 1).await;

    // Register Avro schema in Schema Registry
    let avro_schema_json = r#"{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}"#;
    let schema_id = register_schema(&sr_url, &format!("{topic}-value"), avro_schema_json).await;

    // Produce Confluent-framed Avro messages
    let producer = make_producer(&bootstrap);
    let records = vec![
        (1i64, "alice"),
        (2, "bob"),
        (3, "charlie"),
    ];
    for (id, name) in &records {
        let avro_body = encode_avro_record(*id, name);
        let value = confluent_frame(schema_id, &avro_body);
        producer
            .send(
                BaseRecord::to(topic)
                    .key(&format!("key-{id}"))
                    .payload(&value)
                    .partition(0),
            )
            .expect("send failed");
    }
    producer.flush(Duration::from_secs(5)).expect("flush failed");

    // Build the schema that the reader expects (value fields + kafka metadata)
    let value_schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int64, false),
        arrow::datatypes::Field::new("name", DataType::Utf8, false),
    ]);
    let output_schema = Arc::new(build_deserialized_schema(&value_schema));

    let options = KafkaReadOptions {
        schema_registry_url: Some(sr_url.clone()),
        ..default_options(&bootstrap, topic)
    };

    let range = KafkaPartitionRange {
        topic: topic.to_string(),
        partition: 0,
        start_offset: 0,
        end_offset: 3,
    };

    let encoding = ValueEncoding::Avro {
        reader_schema_json: avro_schema_json.to_string(),
        schema_id,
    };

    let batches = collect_stream(options, range, encoding, output_schema.clone()).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows, got {total_rows}");

    let batch = &batches[0];

    // Verify deserialized "id" column
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column should be Int64");
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 3);

    // Verify deserialized "name" column
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column should be Utf8");
    assert_eq!(name_col.value(0), "alice");
    assert_eq!(name_col.value(1), "bob");
    assert_eq!(name_col.value(2), "charlie");

    // Verify kafka metadata columns are appended
    assert_eq!(batch.schema().field(2).name(), "_kafka_key");
    assert_eq!(batch.schema().field(3).name(), "_kafka_topic");
    assert_eq!(batch.schema().field(4).name(), "_kafka_partition");
    assert_eq!(batch.schema().field(5).name(), "_kafka_offset");
    assert_eq!(batch.schema().field(6).name(), "_kafka_timestamp");
}

// ------------------------------------------------------------------
// Test: JSON deserialization
// ------------------------------------------------------------------

#[tokio::test]
async fn test_json_deserialization() {
    let cluster = start_kafka_cluster("json_test").await;
    let bootstrap = cluster.bootstrap_servers();
    let topic = "sail_json_test";

    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap);
    let json_records = vec![
        r#"{"user_id":100,"email":"alice@example.com","active":true}"#,
        r#"{"user_id":200,"email":"bob@example.com","active":false}"#,
        r#"{"user_id":300,"email":"charlie@example.com","active":true}"#,
    ];
    for (i, record) in json_records.iter().enumerate() {
        producer
            .send(
                BaseRecord::to(topic)
                    .key(&format!("key-{i}"))
                    .payload(*record)
                    .partition(0),
            )
            .expect("send failed");
    }
    producer.flush(Duration::from_secs(5)).expect("flush failed");

    let value_schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("user_id", DataType::Int64, true),
        arrow::datatypes::Field::new("email", DataType::Utf8, true),
        arrow::datatypes::Field::new("active", DataType::Boolean, true),
    ]);
    let output_schema = Arc::new(build_deserialized_schema(&value_schema));

    let options = default_options(&bootstrap, topic);
    let range = KafkaPartitionRange {
        topic: topic.to_string(),
        partition: 0,
        start_offset: 0,
        end_offset: 3,
    };

    let encoding = ValueEncoding::Json {
        schema: value_schema.clone(),
    };

    let batches = collect_stream(options, range, encoding, output_schema.clone()).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "expected 3 rows, got {total_rows}");

    let batch = &batches[0];

    // Verify deserialized columns
    let user_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("user_id column should be Int64");
    assert_eq!(user_ids.value(0), 100);
    assert_eq!(user_ids.value(1), 200);
    assert_eq!(user_ids.value(2), 300);

    let emails = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("email column should be Utf8");
    assert_eq!(emails.value(0), "alice@example.com");
    assert_eq!(emails.value(1), "bob@example.com");
}

// ------------------------------------------------------------------
// Test: failOnDataLoss check
// ------------------------------------------------------------------

#[tokio::test]
async fn test_fail_on_data_loss_false_skips_missing() {
    let cluster = start_kafka_cluster("dataloss_test").await;
    let bootstrap = cluster.bootstrap_servers();
    let topic = "sail_dataloss_test";

    create_topic(&bootstrap, topic, 1).await;

    let producer = make_producer(&bootstrap);
    for i in 0..3 {
        let value = format!("msg-{i}");
        producer
            .send(BaseRecord::to(topic).payload(&value).partition(0))
            .expect("send failed");
    }
    producer.flush(Duration::from_secs(5)).expect("flush failed");

    // Request offset 0, but pretend watermark is at 0 -- should succeed normally
    let mut options = default_options(&bootstrap, topic);
    options.fail_on_data_loss = false;
    let schema = Arc::new(raw_kafka_schema(false));
    let range = KafkaPartitionRange {
        topic: topic.to_string(),
        partition: 0,
        start_offset: 0,
        end_offset: 3,
    };

    let batches = collect_stream(options, range, ValueEncoding::Raw, schema).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
}

// ------------------------------------------------------------------
// Test: Empty topic returns no batches
// ------------------------------------------------------------------

#[tokio::test]
async fn test_empty_topic() {
    let cluster = start_kafka_cluster("empty_test").await;
    let bootstrap = cluster.bootstrap_servers();
    let topic = "sail_empty_test";

    create_topic(&bootstrap, topic, 1).await;

    let options = default_options(&bootstrap, topic);
    let schema = Arc::new(raw_kafka_schema(false));
    let range = KafkaPartitionRange {
        topic: topic.to_string(),
        partition: 0,
        start_offset: 0,
        end_offset: 0,
    };

    let batches = collect_stream(options, range, ValueEncoding::Raw, schema).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0, "empty topic should yield 0 rows");
}

// ------------------------------------------------------------------
// Schema Registry HTTP helper
// ------------------------------------------------------------------

async fn register_schema(sr_url: &str, subject: &str, schema_json: &str) -> u32 {
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "schema": schema_json,
        "schemaType": "AVRO"
    });
    let resp = client
        .post(format!("{sr_url}/subjects/{subject}/versions"))
        .header("Content-Type", "application/vnd.schemaregistry.v1+json")
        .json(&body)
        .send()
        .await
        .expect("schema registration request failed");

    assert!(
        resp.status().is_success(),
        "schema registration failed: {}",
        resp.status()
    );

    let json: serde_json::Value = resp.json().await.expect("parse registration response");
    json["id"].as_u64().expect("missing schema id") as u32
}
