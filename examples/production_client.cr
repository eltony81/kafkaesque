require "../src/kafkaesque"
require "json"

# ==============================================================================
# Examples: Production-Ready Client Settings
# This file demonstrates configuring:
# 1. Producer Memory Budgeting & Backpressure (buffer.memory & max.block.ms)
# 2. Consumer Fetch Size Constraints (fetch.max.bytes & max.partition.fetch.bytes)
# 3. Background Metadata Refresh (topic.metadata.refresh.interval.ms)
# 4. Background OAuthBearer Refresh (sasl.oauthbearer.token.refresh.interval.ms)
# ==============================================================================

# ------------------------------------------------------------------------------
# 1. Production Producer Configuration
# ------------------------------------------------------------------------------
producer_config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  settings: {
    "enable.idempotence" => "true",
    "acks"               => "all",
    "linger.ms"          => "50",
    "batch.num.messages" => "5000",

    # --- Production Ready Enhancements ---
    "buffer.memory"                      => "33554432", # limit accumulator queue to 32MB
    "max.block.ms"                       => "15000",    # block calling fiber up to 15 seconds if buffer is full
    "topic.metadata.refresh.interval.ms" => "300000",   # refresh broker topology every 5 minutes in background
  }
)

producer = Kafkaesque::Producer.new(producer_config)

# Helper monitoring stats callback
producer.on_stats do |stats_json|
  puts "[STATS] Producer metrics: #{stats_json}"
end

# ------------------------------------------------------------------------------
# 2. Production Consumer Configuration
# ------------------------------------------------------------------------------
# In production, OIDC (Keycloak/Okta) access tokens typically expire in 5-15 mins.
# We configure a custom oauth provider endpoint & background token refresh loop.
consumer_config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  group_id: "production-analytics-group",
  settings: {
    "security.protocol"                   => "SASL_PLAINTEXT",
    "sasl.mechanism"                      => "OAUTHBEARER",
    "sasl.oauthbearer.token.endpoint.url" => "http://localhost:8080/realms/kafka/protocol/openid-connect/token",
    "sasl.oauthbearer.client.id"          => "analytics-consumer",
    "sasl.oauthbearer.client.secret"      => "secret-key-12345",

    # --- Production Ready Enhancements ---
    "fetch.max.bytes"                            => "5242880", # max 5MB per network fetch request
    "max.partition.fetch.bytes"                  => "1048576", # max 1MB per partition fetch segment
    "topic.metadata.refresh.interval.ms"         => "300000",  # refresh partition topology every 5 mins in background
    "sasl.oauthbearer.token.refresh.interval.ms" => "300000",  # run background token refresh every 5 mins
  }
)

consumer = Kafkaesque::Consumer.new(consumer_config)

puts "🚀 Production producer & consumer initialized successfully!"
puts "Configs loaded:"
puts " - Producer Buffer Memory limit: #{producer_config.buffer_memory} bytes"
puts " - Consumer Max Fetch Bounds: #{consumer_config.fetch_max_bytes} bytes"
puts " - Consumer Max Partition Fetch Bounds: #{consumer_config.max_partition_fetch_bytes} bytes"

# Cleanly close clients
producer.close
consumer.close
