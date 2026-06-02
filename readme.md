# Kafkaesque

Kafkaesque is a modern, dependency-light Crystal client library for Apache Kafka. It includes support for KIP-848 consumer group protocols, transactional delivery, and pluggable OAuthBearer (OIDC) token authentication out-of-the-box.

---

## Installation

Add this to your application's `shard.yml`:

```yaml
dependencies:
  kafkaesque:
    github: eltony81/kafkaesque
```

Then run `shards install`.

---

## Configuration

Kafkaesque configurations can be constructed programmatically, loaded via a YAML file, or overridden through environment variables (useful for containerized/Docker environments).

### Configuration Loader

To load settings dynamically:

```crystal
require "kafkaesque"

# Automatically looks for config.yml (or custom path) and layers environment variables
config_file = ENV["KAFKA_CONFIG_FILE"]? || "config.yml"
producer_config = Kafkaesque::ConfigLoader.load_producer_config(config_file)
consumer_config = Kafkaesque::ConfigLoader.load_consumer_config(config_file)
```

### Configuration YAML Structure

A complete YAML configuration (`config.yml`):

```yaml
bootstrap_servers:                     # Shared: List of brokers
  - "localhost:9093"
group_id: "my-consumer-group"          # Consumer-only: Consumer group identifier
initial_offset_smallest: "true"        # Consumer-only: "true" resets to earliest offset, "false" to latest
compression_type: "lz4"                # Producer-only: gzip, snappy, lz4, zstd

# Custom settings maps directly under settings
settings:
  # Producer-only settings
  enable.idempotence: "true"
  acks: "all"
  linger.ms: "20"
  batch.num.messages: "10000"
  retries: "5"
  retry.backoff.ms: "100"
  
  # Shared OAuthBearer / OIDC authentication settings
  sasl.oauthbearer.token.endpoint.url: "http://localhost:8080/realms/kafka-auth/protocol/openid-connect/token"
  sasl.oauthbearer.client.id: "kafka-client"
  sasl.oauthbearer.client.secret: "kafka-secret"
```

### Environment Variables

Environment variables take precedence over settings loaded from the YAML file.

- **`KAFKA_BOOTSTRAP_SERVERS`**: Comma-separated list of brokers (e.g. `localhost:9093,localhost:9094`).
- **`KAFKA_GROUP_ID`**: The consumer group identifier.
- **`KAFKA_COMPRESSION_TYPE`**: Compression codec (`gzip`, `snappy`, `lz4`, `zstd`).
- **`KAFKA_INITIAL_OFFSET_SMALLEST`**: Set to `"true"` to start from the earliest offset.
- **`KAFKA_SETTING_<KEY>`**: Any custom setting where `<KEY>` has underscores replaced by dots and is lowercased. E.g., `KAFKA_SETTING_ENABLE_IDEMPOTENCE=true` maps to `enable.idempotence = "true"`.
- **`KAFKA_SASL_<KEY>`**: Any SASL setting. E.g., `KAFKA_SASL_OAUTHBEARER_CLIENT_ID=kafka-client` maps to `sasl.oauthbearer.client.id = "kafka-client"`.

---

## Configuration Parameter Directory

### General & Producer Configuration

| Parameter Key | Type | Default | Description |
|---|---|---|---|
| `bootstrap_servers` | `Array(String)` | `["localhost:9092"]` | Initial list of broker hosts and ports. |
| `compression.type` or `compression_type` | `String` | `none` | Compression codec to use. Choices: `gzip`, `snappy`, `lz4`, `zstd`. |
| `enable.idempotence` | `String` | `false` | Enable idempotent delivery (ensures exactly-once semantics per partition). |
| `acks` | `String` | `1` | Number of broker acknowledgments required before completing write. Options: `all` (-1), `0` (no ack), `1` (leader ack). |
| `linger.ms` | `String` | `0` | Delay (in milliseconds) to wait for additional messages to accumulate before sending a batch. |
| `batch.num.messages` | `String` | `1000` | Maximum number of messages to bundle in a single batch. |
| `retries` | `String` | `0` | Number of times to retry producing a message before failing. |
| `retry.backoff.ms` | `String` | `100` | Time to wait before attempting a retry. |
| `transactional.id` | `String` | `nil` | Unique ID enabling transactional delivery across restarts. |

### Consumer Configuration

| Parameter Key | Type | Default | Description |
|---|---|---|---|
| `group.id` or `group_id` | `String` | `default-group` | Unique string identifying the consumer group. |
| `group.instance.id` | `String` | `nil` | Static member identifier to prevent frequent rebalances. |
| `session.timeout.ms` | `String` | `30000` | Timeout used to detect consumer failures. |
| `heartbeat.interval.ms` | `String` | Auto-calculated | Interval between heartbeats sent to the coordinator. |
| `initial_offset_smallest` | `Bool` | `false` | Corresponds to `auto.offset.reset = smallest` (earliest offset). |
| `auto.offset.reset` | `String` | `largest` | Reset policy: `smallest` (earliest), `largest` (latest). |
| `enable.auto.commit` | `String` | `true` | Periodically commit offsets in the background. |
| `auto.commit.interval.ms` | `String` | `5000` | Interval to auto-commit offsets. |
| `fetch.min.bytes` | `String` | `1` | Minimum data amount the broker should return for a fetch request. |

### SASL & OAuthBearer (OIDC) Settings

| Parameter Key | Type | Default | Description |
|---|---|---|---|
| `sasl.token` / `sasl.password` | `String` | `nil` | Plaintext token or secret for basic SASL connections. |
| `sasl.oauthbearer.token.endpoint.url` | `String` | `nil` | Keycloak/OIDC server URL endpoint to fetch OAuth access tokens. |
| `sasl.oauthbearer.client.id` | `String` | `nil` | The Client ID used for client credentials flow. |
| `sasl.oauthbearer.client.secret` | `String` | `nil` | The Client Secret used for client credentials flow. |

---

## Detailed Tutorials

### 1. Simple Producer

```crystal
require "kafkaesque"

# Load config
config = Kafkaesque::ConfigLoader.load_producer_config("config.yml")
producer = Kafkaesque::Producer.new(config)

begin
  topic = "my-topic"
  payload = "Hello from Kafkaesque!".to_slice
  key = "message_key_1".to_slice
  
  headers = {
    "correlationid" => "12345",
    "client_id"     => "my-producer-app"
  }

  # Write record
  producer.produce(topic, payload, key: key, headers: headers)
  
  # Ensure all batched messages are dispatched
  producer.flush(timeout_ms: 1000)
  puts "Message sent successfully!"
ensure
  producer.close
end
```

### 2. Transactional Producer

Transactions ensure that messages across multiple partitions are written atomically.

```crystal
require "kafkaesque"

# Setup configuration with a transactional ID
config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9093"],
  settings: {
    "transactional.id" => "tx-prod-1",
    "enable.idempotence" => "true"
  }
)
producer = Kafkaesque::Producer.new(config)

begin
  # Begin transactional scope
  producer.begin_transaction

  producer.produce("topic-a", "Payload A".to_slice)
  producer.produce("topic-b", "Payload B".to_slice)

  # Commit both writes atomically
  producer.commit_transaction
  puts "Transaction committed successfully!"
rescue ex
  # Rollback changes on failure
  producer.abort_transaction
  puts "Transaction aborted: #{ex.message}"
ensure
  producer.close
end
```

### 3. Simple Consumer

The consumer uses standard fibers and blocks the loop until messages arrive.

```crystal
require "kafkaesque"

config = Kafkaesque::ConfigLoader.load_consumer_config("config.yml")
consumer = Kafkaesque::Consumer.new(config)

consumer.subscribe(["my-topic"])

# Register rebalance event callbacks (Optional)
consumer.on_partitions_assigned do |partitions|
  puts "Partitions assigned: #{partitions.inspect}"
end

consumer.on_partitions_revoked do |partitions|
  puts "Partitions revoked: #{partitions.inspect}"
end

# Setup termination signal handling
spawn do
  Process.on_terminate do
    puts "Shutdown requested..."
    consumer.close
    exit
  end
end

begin
  # Block and consume events sequentially
  consumer.each do |message|
    puts "Offset: #{message.offset} | Key: #{message.key} | Value: #{message.value}"
  end
ensure
  consumer.close
end
```

---

## Supported Kafka Protocol Versions & Features

Kafkaesque implements a native Crystal serialization engine that directly communicates with Kafka brokers. The table below lists the API keys, protocol versions used under-the-hood, and associated features:

| API Key | API Name | Protocol Version | Features / Implementation Notes |
| :---: | :--- | :---: | :--- |
| **0** | `Produce` | `v7` | Supports message headers, record batching, idempotence metadata (`producer_id`, `producer_epoch`), and transactional envelopes. |
| **1** | `Fetch` | `v4` | Downloads record batches with key/value extraction and header parsing. |
| **2** | `ListOffsets` | `v1` | Retrieves logical partition boundary offsets (earliest/latest). |
| **3** | `Metadata` | `v2` | Resolves topic-partition topology and maps partition leader hosts. |
| **8** | `OffsetCommit` | `v2` | Commits individual partition consumer group offsets to coordinator brokers. |
| **9** | `OffsetFetch` | `v1` | Queries the current group's committed partition offsets. |
| **10** | `FindCoordinator` | `v2` | Resolves coordinator node endpoints for dynamic consumer groups. |
| **11** | `JoinGroup` | `v0` | Used during legacy consumer group join. |
| **14** | `Heartbeat` | `v1` | Keeps legacy consumer dynamic membership heartbeat active. |
| **17** | `SaslHandshake` | `v1` | Initiates authentication protocols. |
| **36** | `SaslAuthenticate` | `v1` | Passes dynamic tokens (Plain or OAuthBearer OIDC access tokens) to the broker. |
| **22** | `InitProducerId` | `v0` | Fetches a transactional producer ID and current epoch. |
| **24** | `AddPartitionsToTxn` | `v0` | Registers partitions inside an active transactional transaction context. |
| **26** | `EndTxn` | `v0` | Atomically commits or aborts a multi-partition transaction scope. |
| **84** | `ConsumerGroupHeartbeat`| `v1` | **KIP-848 Next-Gen Consumer Group Coordination**: Implements server-side partition assignments, rolling memberships, and dynamic balance loops. |

### Features Summary
1. **Next-Generation Consumer Protocol**: Out-of-the-box support for **KIP-848** (Consumer Group Heartbeat v1) to minimize client-side rebalance complexities and connection storms.
2. **Exactly-Once Semantics (EOS)**: Support for transactional writes and idempotent producers.
3. **Container-Oriented Design**: Fully configurable through declarative YAML files and container environment variables.
4. **Native Authentication**: Support for SASL Plaintext and dynamic OAuthBearer/OIDC (Keycloak, Okta, etc.) credential fetching under-the-hood.

---

## License

This project is licensed under the MIT License.
