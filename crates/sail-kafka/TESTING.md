# Testing sail-kafka Locally

## Prerequisites

- Docker (Docker Desktop or colima/orbstack on macOS)
- Rust toolchain (1.88.0+)
- CMake (required by `rdkafka` with `cmake-build` feature)

```bash
# macOS
brew install cmake

# verify docker is running
docker info
```

## Option 1: Automated Integration Tests (testcontainers)

The integration tests use `testcontainers` to spin up Kafka and Schema Registry
containers automatically. No manual Docker setup needed.

```bash
cd sail

# Run all integration tests
cargo test -p sail-kafka --test kafka_integration_test -- --test-threads=1

# Run a single test
cargo test -p sail-kafka --test kafka_integration_test test_raw_mode_read
cargo test -p sail-kafka --test kafka_integration_test test_avro_confluent_deserialization
cargo test -p sail-kafka --test kafka_integration_test test_json_deserialization
```

> **Note**: Use `--test-threads=1` to avoid port conflicts between tests that
> each start their own Kafka cluster. Each test takes ~15s due to container
> startup (Kafka KRaft init + Schema Registry).

### What the tests cover

| Test | What it validates |
|------|-------------------|
| `test_raw_mode_read` | Produce 5 messages, read via `kafka_partition_stream` in raw binary mode, verify all 7 schema columns, byte content, sequential offsets |
| `test_raw_mode_multi_partition` | 3 partitions x 4 messages, reads each partition independently |
| `test_avro_confluent_deserialization` | Registers Avro schema in Schema Registry, produces Confluent-framed messages (`0x00` + schema_id + Avro binary), reads via `arrow-avro::Decoder`, verifies deserialized columns + kafka metadata |
| `test_json_deserialization` | Produces JSON records, reads via `arrow-json`, verifies deserialized columns |
| `test_fail_on_data_loss_false_skips_missing` | Verifies `failOnDataLoss=false` does not error |
| `test_empty_topic` | Empty partition range returns 0 rows |

## Option 2: Manual Testing with docker-compose

For interactive debugging or ad-hoc testing against a long-lived cluster.

### 1. Start the cluster

```bash
cd sail/crates/sail-kafka
docker compose -f docker-compose.test.yml up -d
```

This starts:
- **Kafka** (KRaft mode, no ZooKeeper) on `localhost:9092`
- **Schema Registry** on `localhost:8081`

Wait for healthy status:

```bash
docker compose -f docker-compose.test.yml ps
# Both should show "healthy"
```

### 2. Create a test topic

```bash
docker exec sail-kafka-test \
  kafka-topics --create \
    --topic test-topic \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server localhost:29092
```

### 3. Produce test messages

**Raw text messages:**

```bash
docker exec -i sail-kafka-test \
  kafka-console-producer \
    --topic test-topic \
    --bootstrap-server localhost:29092 \
    --property "key.separator=:" \
    --property "parse.key=true" <<EOF
key1:{"user_id":1,"name":"alice"}
key2:{"user_id":2,"name":"bob"}
key3:{"user_id":3,"name":"charlie"}
EOF
```

**Avro messages with Schema Registry:**

```bash
# Register a schema
curl -X POST http://localhost:8081/subjects/test-avro-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"}]}",
    "schemaType": "AVRO"
  }'
# Returns: {"id":1}

# Produce Avro messages (requires kafka-avro-console-producer)
docker exec -i sail-kafka-test \
  kafka-avro-console-producer \
    --topic test-avro \
    --bootstrap-server localhost:29092 \
    --property schema.registry.url=http://schema-registry:8081 \
    --property value.schema='{"type":"record","name":"TestRecord","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"}]}' <<EOF
{"id":1,"name":"alice"}
{"id":2,"name":"bob"}
EOF
```

### 4. Verify with console consumer

```bash
docker exec sail-kafka-test \
  kafka-console-consumer \
    --topic test-topic \
    --from-beginning \
    --bootstrap-server localhost:29092 \
    --max-messages 3
```

### 5. Verify Schema Registry

```bash
# List subjects
curl http://localhost:8081/subjects

# Get latest schema for a subject
curl http://localhost:8081/subjects/test-avro-value/versions/latest

# Get schema by ID
curl http://localhost:8081/schemas/ids/1
```

### 6. Tear down

```bash
docker compose -f docker-compose.test.yml down -v
```

## Troubleshooting

### `rdkafka` build fails with CMake errors

```bash
# Ensure cmake is installed and on PATH
cmake --version

# On macOS with Homebrew
brew install cmake pkg-config
```

### Kafka container fails to start

```bash
# Check logs
docker compose -f docker-compose.test.yml logs kafka

# Common issue: KAFKA_CLUSTER_ID must be exactly 22 base64 characters
# The docker-compose.test.yml uses a pre-set valid ID
```

### Tests hang or timeout

Each testcontainers test waits ~8s for Kafka and ~3s for Schema Registry to
initialize. If your Docker host is slow, increase the sleep durations in
`start_kafka_cluster()` in the test file.

### Port conflicts

The testcontainers tests use `with_mapped_port` with ports derived from the
test name hash (range 19092-29091). If you have other services on those ports,
stop them first. The docker-compose setup uses fixed ports 9092 and 8081.

### Schema Registry can't connect to Kafka

Schema Registry connects to Kafka via Docker's bridge network IP. If the
`get_bridge_ip_address()` call fails, ensure Docker networking is working:

```bash
docker network ls
docker network inspect bridge
```

## Architecture of the test

```
                  ┌─────────────────┐
                  │  Test Process    │
                  │  (cargo test)   │
                  │                 │
                  │  rdkafka ───────┼──── localhost:<mapped_port> ──┐
                  │  producer       │                               │
                  │                 │                               ▼
                  │  kafka_         │                    ┌──────────────────┐
                  │  partition_     │                    │  Kafka Container │
                  │  stream() ─────┼──── localhost:<mp> │  (KRaft mode)    │
                  │  (consumer)    │                    │  EXTERNAL :9092  │
                  │                 │                    │  BROKER   :29092 │
                  │  reqwest ──────┼──── localhost:<sp> │                  │
                  │  (SR client)   │         │          └──────────────────┘
                  └─────────────────┘         │                    ▲
                                              ▼                    │ :29092
                                   ┌──────────────────┐            │
                                   │ Schema Registry   │ ──────────┘
                                   │ Container :8081   │
                                   └──────────────────┘
```

The test produces messages via `rdkafka::BaseProducer`, then reads them back
through the `kafka_partition_stream()` function (the same code path used by
`KafkaScanExec` in production). This validates the full read pipeline:
consumer init, offset seeking, message polling, and deserialization (raw /
Avro via `arrow-avro::Decoder` / JSON via `arrow-json`).
