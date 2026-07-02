# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Kafkaesque (`kafkaesque.cr`) is a dependency-light Apache Kafka client library for Crystal. It implements the Kafka wire protocol from scratch (no librdkafka binding) — TCP framing, request/response serialization, SASL/SCRAM/OAuthBearer auth, and modern KIPs (KIP-848 consumer groups, KIP-932 share groups, KIP-714 telemetry, KIP-392 closest-replica reads) — targeting high throughput via Crystal's fiber-based cooperative concurrency.

> Per the readme, this library is purpose-built to support the [cryspace](https://github.com/eltony81/cryspace) framework and is explicitly *not* vetted for general-purpose production use. Keep that framing in mind when evaluating trade-offs (e.g. it favors raw throughput over exhaustive edge-case hardening).

## Commands

```bash
shards install                    # install dependencies (declared in shard.yml)
crystal spec                      # run the full spec suite
crystal spec spec/kip_spec.cr     # run a single spec file
crystal spec spec/kip_spec.cr -e "KIP-392 Closest Replica Routing"  # filter by example/describe name
crystal tool format               # auto-format source
crystal tool format --check       # format check (what CI runs)
crystal build src/kafkaesque.cr   # compile-check the library
```

CI (`.github/workflows/ci.yml`) runs on Crystal 1.20.2 and requires `libsnappy-dev liblz4-dev libzstd-dev` (plus `libssl-dev` for TLS) as system libraries for the native compression/SSL bindings to link. It runs `crystal tool format --check` then `crystal spec` — always run both before considering a change done.

Specs do **not** require a real Kafka broker: they spin up `Kafkaesque::MockBroker` (`src/kafkaesque/mock_broker.cr`), an in-process `TCPServer` on a random port with per-API-key handler registration (`mock.on_request(api_key) { |decoder, correlation_id| ... }`), and use it to drive `Client`/`Consumer`/`Producer` through real protocol round-trips instead of mocking Crystal objects directly. Follow this pattern for new integration-style specs.

## Architecture

### Layering

```
Kafkaesque::Client            connection lifecycle, auth, batching, partition routing, prefetching
  ├── src/kafkaesque/client/*.cr   split by concern via `require "./client/*"` in client.cr:
  │     group.cr           consumer group coordination (KIP-848 heartbeat, join/sync)
  │     offset.cr          ListOffsets / OffsetCommit / OffsetFetch
  │     produce_fetch.cr   Produce/Fetch request building, record batching, share-group fetch
  │     transaction.cr     InitProducerId / AddPartitionsToTxn / EndTxn
  ├── Connection             one TCP/SSL socket + framing (4-byte length prefix) + mutex-protected write/read
  └── Protocol::*            wire format: Encoder/Decoder (types.cr), per-API request/response structs
Kafkaesque::Consumer / Producer   user-facing API; each owns a Client and a Config
```

`Client` is the real engine — it is *not* just a thin socket wrapper. It owns:
- **Broker connection pooling**: `@broker_connections : Hash(Int32, Connection)` keyed by node ID, established lazily via `connection_for_partition` / `get_or_establish_broker_connection`, with retry+backoff (`Backoff`, `src/kafkaesque/backoff.cr`) on failure.
- **Partition metadata cache**: `@partition_leaders` / `@partition_replicas` (`"topic:partition" => node_id`), refreshed on cache miss and periodically by a background fiber (`start_metadata_refresh_fiber`).
- **Producer batching**: `@pending_batch : Hash(Tuple(String, Int32), Array(Protocol::Record))`, flushed by a linger-driven background fiber for O(1) accumulation.
- **Consumer prefetching**: `@prefetch_channel : Channel(Protocol::Record)` fed by per-partition background fibers; `Consumer#each` just pulls off this channel, decoupling network I/O from user processing.
- **Idempotent/transactional producer state**: `producer_id`, `producer_epoch`, per-partition `@sequence_numbers`.
- Background fibers for OAuth token refresh, dynamic mTLS keystore reload, and the optional Prometheus metrics HTTP server (`MetricsServer`) — all started/stopped from `connect`/`close`.

### Protocol layer (`src/kafkaesque/protocol/`)

Each file implements one or a few related Kafka API keys as plain structs with `.serialize(encoder)` / `self.deserialize(decoder)`, using the shared `Protocol::Encoder`/`Protocol::Decoder` (`types.cr`) for primitives (fixed-width ints, zigzag varints/varlongs, compact/flexible-version encoding, tagged field buffers). `request.cr`/`response.cr` hold the common `RequestHeader`/`ResponseHeader`. New API support should follow the existing per-file, per-API-key struct pattern rather than a generic dynamic codec — see the API key/version table in `readme.md` ("Supported Kafka Protocol Versions & Features") for what's implemented and at which version.

Compression codecs (gzip/snappy/lz4/zstd) are in `protocol/compression.cr` and link against system `libsnappy`/`liblz4`/`libzstd`.

### Concurrency model

Everything is cooperative Crystal fibers, not OS threads/preemption. Shared mutable state crossing fiber boundaries is guarded by explicit `Mutex`es (e.g. `@hb_mutex`, `@offset_mutex`, `@batch_mutex`, `Connection`'s per-socket mutex). When adding background loops, follow the existing `start_x_fiber` / `stop_x_fiber` + boolean `@x_running` flag convention used throughout `client.cr` so `close` can shut everything down deterministically.

### Configuration

`Producer::Config` / `Consumer::Config` wrap a flat `Hash(String, String)` of dotted Kafka-style settings (e.g. `"linger.ms"`) with typed accessor properties on top (`linger_ms=`, `acks=`, etc. — see `producer.cr`/`consumer.cr`). `ConfigLoader` (`config_loader.cr`) layers a YAML file under `KAFKA_*` environment variables (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SETTING_<KEY>`, `KAFKA_SASL_<KEY>`, `KAFKA_SSL_<KEY>`) to produce these configs — env vars always win. When adding a new setting, wire it through both the YAML/env loader *and* a typed accessor if one is warranted, matching existing settings.

### Examples

`examples/*.cr` are runnable, self-contained demonstrations of individual features (transactions, SCRAM/mTLS, share groups, telemetry, manual assignment, custom partitioners, etc.) — check there before writing new usage docs, and keep them in sync when changing public APIs they exercise.
