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
| `retries` | `String` | `3` | Number of times to retry producing a message before failing. |
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

### 4. Regex Consumer Subscription

You can subscribe to topics dynamically matching a Regular Expression. A background discovery loop automatically identifies newly created topics in the cluster that match the pattern and subscribes the consumer to them.

```crystal
require "kafkaesque"

config = Kafkaesque::ConfigLoader.load_consumer_config("config.yml")
consumer = Kafkaesque::Consumer.new(config)

# Subscribe to any topic starting with "sensor-"
consumer.subscribe(/^sensor-.*$/)

begin
  consumer.each do |message|
    puts "Topic: #{message.topic} | Value: #{message.value}"
  end
ensure
  consumer.close
end
```

### 5. Manual Partition Assignment

If you need to consume from a specific set of partitions without dynamic consumer group partition assignment (rebalances) or group heartbeats, you can assign them manually using `Consumer#assign`.

```crystal
require "kafkaesque"

config = Kafkaesque::Consumer::Config.new(["localhost:9092"])
consumer = Kafkaesque::Consumer.new(config)

# Manually assign partition 0 and 1 of "my-topic"
consumer.assign([
  Kafkaesque::TopicPartition.new("my-topic", 0),
  Kafkaesque::TopicPartition.new("my-topic", 1)
])

begin
  consumer.each do |message|
    puts "Topic: #{message.topic} | Partition: #{message.partition} | Value: #{message.value}"
  end
ensure
  consumer.close
end
```

### 6. Unit Testing with Mock Broker

Kafkaesque provides a built-in `MockBroker` to verify your application's consumer or producer logic locally without needing a live Kafka container.

```crystal
require "spec"
require "kafkaesque"
require "kafkaesque/mock_broker"

describe "My Kafka Application" do
  it "successfully publishes message to mock broker" do
    # Start mock broker on random local port
    broker = Kafkaesque::MockBroker.new

    # Mock response for Produce requests (API KEY 0)
    broker.on_request(0_i16) do |decoder, version|
      # Parse or skip request details as desired
      # and return a serialized ProduceResponse body
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      # Array of topics (size 1)
      enc.write_array(["my-topic"]) do |topic|
        enc.write_string(topic)
        # Array of partitions (size 1)
        enc.write_array([0]) do |part|
          enc.write_int32(part)    # Partition index
          enc.write_int16(0_i16)   # Success error code
          enc.write_int64(42_i64)  # Committed base offset
          enc.write_int64(-1_i64)  # Log append time
          enc.write_int64(0_i64)   # Log start offset
        end
      end
      enc.write_int32(0) # throttle_time_ms
      io
    end

    begin
      # Direct client to connect to local mock broker
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      # Produce message
      resp = client.produce("my-topic", "key", "val")
      resp.error_code.should eq(0)
      resp.base_offset.should eq(42)
    ensure
      broker.close
    end
  end
end
```

---

## API Reference

### `Kafkaesque::ConfigLoader`
Static utility module to load configurations.
* **`self.load_producer_config(file_path : String) : Producer::Config`**: Reads a YAML file and overrides configurations using system environment variables.
* **`self.load_consumer_config(file_path : String) : Consumer::Config`**: Same as above, returned as a Consumer configuration.

---

### `Kafkaesque::Client`

Underlying connection and protocol routing manager client. Handles raw TCP sockets, SASL authentication, metadata refreshes, and partition leader routing.

#### Constructor
* **`Client.new(host : String, port : Int32, use_ssl : Bool = false, sasl_token : String? = nil, client_id = "kafkaesque-crystal", ssl_context : OpenSSL::SSL::Context::Client? = nil, oauth_token_provider : (-> String)? = nil, max_retries : Int32 = 3)`**: Initializes a new client connection. `max_retries` configures the retry limit (defaults to `3`) for self-healing routing when partition leader changes occur or connection exceptions are encountered.

#### Static Methods
* **`Client.connect_first(servers : Array(String), sasl_token : String? = nil, client_id : String = "kafkaesque-crystal", oauth_token_provider : (-> String)? = nil, use_ssl : Bool = false, ssl_context : OpenSSL::SSL::Context::Client? = nil, max_retries : Int32 = 3) : Client`**: Attempts connecting sequentially to a list of bootstrap servers, returning the first successful connection.

---

### `Kafkaesque::Producer`

High-throughput, asynchronous client to write records to Kafka brokers.

#### Constructor
* **`Producer.new(config : Config)`**: Initializes a new producer with the given configuration.
* **`Producer.new(&block : Config ->)`**: Block builder syntax initializing the config before constructing.

#### Public Methods
* **`produce(topic : String, payload : Bytes | String, key : Bytes | String? = nil, headers : Hash(String, String) = {}, partition : Int32 = 0, timestamp : Time? = nil)`**: Asynchronously queues a record into the batch accumulator. Automatically handles retries and backoffs if configured.
* **`flush(timeout_ms : Int32 = 5000)`**: Forces the batch accumulator to immediately serialize and write all queued records to the broker network socket.
* **`begin_transaction`**: Starts a transactional scope (requires `transactional.id` to be defined in configurations).
* **`commit_transaction`**: Atomically commits all produced records written inside the active transaction scope.
* **`abort_transaction`**: Aborts and discards all records written inside the active transaction scope.
* **`send_offsets_to_transaction(offsets : Hash(String, Int64), group_id : String)`**: Commits consumer group offsets inside the transaction context (enables exactly-once transactional consumer-producer flows).
* **`on_deliver(&block : String, Int32, Int64, Exception? -> Void)`**: Registers a callback block executed whenever a message is successfully delivered or fails. Block parameters are: `topic`, `partition`, `offset`, `exception`.
* **`on_stats(&block : String -> Void)`**: Registers a callback block reporting periodic diagnostic and state metadata.
* **`close`**: Flushes remaining batches and closes TCP socket connections.

---

### `Kafkaesque::Consumer`

Evented streaming client supporting server-side KIP-848 partition coordination.

#### Constructor
* **`Consumer.new(config : Config)`**: Initializes a new consumer.
* **`Consumer.new(&block : Config ->)`**: Block builder syntax initializing the config before constructing.

#### Public Methods
* **`subscribe(topics : Array(String))`** / **`subscribe(*topics : String)`**: Subscribes the consumer to one or more topics.
* **`assign(topic_partitions : Array(TopicPartition))`** / **`assign(topic_partition : TopicPartition)`**: Manually assigns the consumer to specific topic-partition pairs, bypassing consumer group coordination.
* **`each(&block : Protocol::Record ->)`**: Starts the partition fetch loops on background fibers, initiates membership heartbeat loop (if subscribed to a consumer group), and blocks the current fiber streaming received records sequentially to the block.
* **`on_partitions_assigned(&block : Array(Int32) -> Void)`**: Callback triggered when the broker coordinator assigns partition ownership to the consumer member.
* **`on_partitions_revoked(&block : Array(Int32) -> Void)`**: Callback triggered when ownership of assigned partitions is revoked.
* **`close`**: Leaves the consumer group cleanly, closes prefetch channels, and terminates connection sockets.

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
4. **Thread-Safe Object Pooling**:
   To minimize Garbage Collector heap allocation pressure under high stress, Kafkaesque utilizes a thread-safe `ObjectPool` to reuse `IO::Memory` serialization buffers during record and batch dispatches.

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

Here is a performance comparison of Kafkaesque (pure Crystal) against Go Confluent (`confluent-kafka-go` wrapping `librdkafka`) and **Franz-Go** (`github.com/twmb/franz-go` pure Go library).

### 🖥️ Benchmark Environment & Hardware
* **CPU**: 8-Core AMD Ryzen / Intel Core (Hyper-Threaded, Local Host Execution)
* **RAM**: 16 GB DDR4
* **OS**: Linux (Fedora/Ubuntu) with Podman container virtualization
* **Kafka Instance**: Single-node Kafka broker (version 3.7+) running inside a container, exposed on port `9097` (`PLAINTEXT` listener).

### 📦 Test Data Payload
The stress benchmark transmits **1,000,000 messages**, each carrying a complex JSON telemetry payload representing real-time sensor metrics:
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
  - `linger.ms`: `100`
  - `batch.num.messages`: `10000`
  - Go Confluent optimized with delivery reports disabled (`"go.delivery.reports": false`).
  - All clients use asynchronous queuing and are synchronously flushed exactly once at the end of the 1,000,000-message loop.
* **Consumer settings**:
  - `group.protocol`: `consumer` (Next-generation **KIP-848** membership protocol, supported by Kafkaesque, Go Confluent, and Franz-Go).
  - `fetch.min.bytes`: `1`
  - `fetch.wait.max.ms` / `fetch.max.wait.ms`: `5ms` (for `librdkafka` clients).
  - Kafkaesque runs its background prefetching engine on Crystal fibers.
  - Franz-Go / Go Confluent rely on Go's internal scheduling channels.

---

### 1. Single-Core Results (Pinned to Core 2)

#### Producer Scoreboard
| Rank | Client Engine | Execution Time | Throughput | Peak RAM (RSS) |
| :---: | :--- | :---: | :---: | :---: |
| #1 | Franz-Go | 3.74s | 267,380.0 msg/s | 38.64 MB |
| #2 | Kafkaesque (Single Thread) | 4.72s | 211,864.0 msg/s | 2,571.26 MB |
| #3 | Kafkaesque (Multithread) | 7.29s | 137,174.0 msg/s | 149.23 MB |
| #4 | Go Confluent | 7.49s | 133,511.0 msg/s | 75.38 MB |

#### Consumer Scoreboard
| Rank | Client Engine | Execution Time | Throughput | Peak RAM (RSS) |
| :---: | :--- | :---: | :---: | :---: |
| #1 | Kafkaesque (Single Thread) | 0.72853s | 1,372,627.1 msg/s | 49.14 MB |
| #2 | Kafkaesque (Multithread) | 1.13612s | 880,186.6 msg/s | 142.75 MB |
| #3 | Franz-Go | 1.29074s | 774,748.9 msg/s | 41.38 MB |
| #4 | Go Confluent | 8.95259s | 111,699.6 msg/s | 84.87 MB |

---

### 2. Multi-Core Results (Pinned to Cores 0-7)

#### Producer Scoreboard
| Rank | Client Engine | Execution Time | Throughput | Peak RAM (RSS) |
| :---: | :--- | :---: | :---: | :---: |
| #1 | Franz-Go | 3.58s | 279,330.0 msg/s | 33.83 MB |
| #2 | Kafkaesque (Multithread) | 4.52s | 221,239.0 msg/s | 147.37 MB |
| #3 | Kafkaesque (Single Thread) | 4.72s | 211,864.0 msg/s | 2,571.39 MB |
| #4 | Go Confluent | 4.76s | 210,084.0 msg/s | 89.80 MB |

#### Consumer Scoreboard
| Rank | Client Engine | Execution Time | Throughput | Peak RAM (RSS) |
| :---: | :--- | :---: | :---: | :---: |
| #1 | Kafkaesque (Multithread) | 0.32266s | 3,099,280.3 msg/s | 140.48 MB |
| #2 | Kafkaesque (Single Thread) | 0.76721s | 1,303,429.9 msg/s | 49.58 MB |
| #3 | Franz-Go | 1.04266s | 959,085.9 msg/s | 40.53 MB |
| #4 | Go Confluent | 8.62552s | 115,935.1 msg/s | 83.24 MB |

---

## License

This project is licensed under the MIT License.
