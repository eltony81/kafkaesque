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

## System Dependencies & Compilation

Kafkaesque supports TLS/SSL connections and high-performance compression codecs (Snappy, LZ4, Zstandard) via native C bindings. The compiler requires these dependency libraries to link successfully.

### 🐧 Linux (Ubuntu/Debian)

Install the development packages using your package manager:
```bash
sudo apt-get install libssl-dev libsnappy-dev liblz4-dev libzstd-dev
```
Then build your application:
```bash
crystal build src/your_app.cr --release
```

### 🪟 Windows

On Windows, the Crystal compiler uses the MSVC linker. You need to install and link the libraries using `vcpkg` or `MSYS2`.

#### 1. Dynamic Linking (using `vcpkg`)
Install the package dependencies:
```cmd
vcpkg install openssl:x64-windows snappy:x64-windows lz4:x64-windows zstd:x64-windows
vcpkg integrate install
```
Then compile standardly (MSVC will auto-detect the linked libraries):
```cmd
crystal build src/your_app.cr --release
```

#### 2. Static Linking (Standalone Executable)
To generate a self-contained `.exe` without requiring external DLLs, pass the static libraries as link flags:
```cmd
crystal build src/your_app.cr --release --link-flags="lz4_static.lib zstd_static.lib snappy_static.lib libssl.lib libcrypto.lib"
```

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

## Performance & Optimizations

Kafkaesque is designed to be highly performant by leveraging Crystal's cooperative concurrency:

1. **Direct Socket Writing & `TCP_NODELAY`**:
   Kafkaesque disables Nagle's algorithm (`tcp_nodelay = true`) on broker connections. Because the library manually manages record batching at the application level, this eliminates socket latency without generating tiny, fragmented network packets.
2. **Event-Driven Asynchronous Prefetching**:
   The `Consumer` incorporates an asynchronous prefetch engine. Messages from assigned partitions are fetched in the background by dedicated partition fibers and pushed to an internal channel, allowing the consumption loop (`Consumer#each`) to stream records without sleep delays or polling latency.
3. **$O(1)$ Batch Accumulation**:
   Record batches are accumulated using a partition-keyed hash map lookup, dropping producer queuing times from $O(N)$ linear scans to $O(1)$.

---

## Developer Guide & Diagnostics

### Concurrency & Fiber Safety
Kafkaesque leverages Crystal's cooperative concurrency model (Fibers) and evented socket I/O.
- The consumer loop (`Consumer#each`) runs in a non-blocking fashion.
- Coordination loops (such as heartbeats) execute in a background fiber.
- Shared resources like offsets and rebalance assignment maps are protected internally using mutual exclusions (`@hb_mutex` and `@offset_mutex`).

### Logging & Diagnostics
The library routes diagnostic information using Crystal's standard `Log` engine under the `kafkaesque` namespace rather than printing to `STDOUT`.

To enable detailed logging for connection states, rebalances, and transactions, configure logging in your application entrypoint:
```crystal
require "log"

# Enable debug logging for the library
Log.setup(:debug)
```

### Resiliency & Error Recovery
- **Connection Drops**: If the connection to the broker is lost, the consumer/producer automatically attempts to reconnect.
- **Offset Out Of Range**: If a consumer queries an expired offset, it catches the error and auto-resets by querying partition boundaries using `ListOffsets`.
- **Retries**: Producers retry failed dispatches based on the configured `retries` and `retry.backoff.ms` settings.


## Benchmarks

Here is a performance comparison of Kafkaesque (pure Crystal) against Go Confluent (`confluent-kafka-go` wrapping `librdkafka`), Crafka (Crystal C-wrapper), and **Franz-Go** (`github.com/twmb/franz-go` pure Go library).

### 🖥️ Benchmark Environment & Hardware
* **CPU**: 8-Core Intel Core i7 / AMD Ryzen (Hyper-Threaded, Local Host Execution)
* **RAM**: 16 GB DDR4
* **OS**: Linux (Fedora/Ubuntu) with Podman container virtualization
* **Kafka Instance**: Single-node Kafka broker (version 3.7+) running inside a container, exposed on port `9097` (`PLAINTEXT` listener).

### 📦 Test Data Payload
The test benchmark transmits **100 messages**, each carrying a complex JSON telemetry payload representing real-time sensor metrics:
```json
{
  "message_index": 42,
  "event_type": "sensor_reading",
  "timestamp": "2026-06-02T08:00:00Z",
  "data": {
    "temperature": 27.34,
    "humidity": 58.12,
    "status": "active"
  }
}
```
Along with the payload, each message is accompanied by metadata key string `"sensor_<index>"` and three custom headers:
* `correlationid`: Unique UUIDv4 string
* `client_id`: `"kafkaclitest-producer"`
* `app_version`: `"1.0.0"`

### ⚙️ Client Configurations
* **Topic Setup**: `test-topic` configured with exactly **1 partition** and a replication factor of **1**.
* **Producer settings**:
  - `acks`: `all`
  - `enable.idempotence`: `true`
  - `compression.type`: `lz4`
  - `linger.ms`: `20`
  - `batch.num.messages`: `10000`
  - Go Confluent optimized with delivery reports disabled (`"go.delivery.reports": false`).
  - All clients use asynchronous queuing and are synchronously flushed exactly once at the end of the 100-message loop.
* **Consumer settings**:
  - `group.protocol`: `consumer` (Next-generation **KIP-848** membership protocol, supported by Kafkaesque, Go Confluent, and Franz-Go; Crafka runs on `classic` group protocol).
  - `fetch.min.bytes`: `1`
  - `fetch.wait.max.ms` / `fetch.max.wait.ms`: `5ms` (for `librdkafka` clients).
  - Kafkaesque runs its background prefetching engine on Crystal fibers utilizing a batch-level Crystal channel (delivering pre-buffered record batches directly to the consumption loop to eliminate per-message fiber context-switching overhead).
  - Franz-Go / Go Confluent rely on `librdkafka`'s internal C-thread prefetch queues (configured via `queued.min.messages`, defaulting to 100,000 messages).

---

### 📤 Producer Throughput (100 messages)

| Client Engine | Language | Native / Wrapper | Execution Time | Throughput |
| :--- | :--- | :---: | :---: | :---: |
| **Kafkaesque** | **Crystal** | **Pure Native** | **0.012s** | **8,333.3 msg/s** |
| **Franz-Go** | **Go** | **Pure Native** | **0.043s** | **2,325.6 msg/s** |
| **Go Confluent** | **Go** | C-Wrapper (`librdkafka`) | **0.114s** | **877.2 msg/s** |
| **Crafka** | **Crystal** | C-Wrapper (`librdkafka`) | **0.528s** | **189.4 msg/s** |

### 📥 Consumer Throughput (100 messages)
*Note: To isolate network transport and client serialization capabilities from the broker's coordinator lookup/rebalance protocols, consumer benchmarks measure the duration starting from receipt of the first message.*

| Client Engine | Language | Group Protocol | Execution Time | Throughput |
| :--- | :--- | :---: | :---: | :---: |
| Franz-Go | Go | KIP-848 (Next-Gen) | <0.001s | 2,974,066.1 msg/s |
| Go Confluent | Go | KIP-848 (Next-Gen) | <0.001s | 95,593.7 msg/s |
| Crafka | Crystal | Classic | 0.001s | 79,288.1 msg/s |
| Kafkaesque (Optimized) | Crystal | KIP-848 (Next-Gen) | <0.002s | 70,314.7 msg/s (1,804,565.6 msg/s peak) |

---

## License

This project is licensed under the MIT License.
