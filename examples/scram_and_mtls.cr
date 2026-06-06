require "../src/kafkaesque"

# This example demonstrates how to configure Kafkaesque for production security requirements:
# 1. SASL SCRAM-SHA-256 or SCRAM-SHA-512 authentication.
# 2. Client Mutual TLS (mTLS) with custom certificate/key paths.
# 3. Connection retry with exponential backoff and jitter.

# 1. Configure the security settings hash
settings = {
  # Enable TLS + SASL Authentication
  "security.protocol" => "SASL_SSL",
  "sasl.mechanism"    => "SCRAM-SHA-256", # Or "SCRAM-SHA-512"
  "sasl.username"     => "enterprise-app",
  "sasl.password"     => "super-secure-scram-password",

  # Truststore CA file to verify the brokers
  "ssl.truststore.location" => "/etc/ssl/certs/kafka-ca.pem",

  # Mutual TLS (mTLS) Client credentials
  "ssl.keystore.location"     => "/etc/ssl/certs/client.crt",
  "ssl.keystore.key.location" => "/etc/ssl/certs/client.key",

  # Custom retry limit
  "retries" => "5",
}

# 2. Setup the Producer config
producer_config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["secure-broker-1:9093", "secure-broker-2:9093"],
  settings: settings
)

puts "[INIT] Creating Secure Producer..."
producer = Kafkaesque::Producer.new(producer_config)

# 3. Setup the Consumer config
consumer_config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["secure-broker-1:9093", "secure-broker-2:9093"],
  group_id: "secure-consumer-group",
  settings: settings
)

puts "[INIT] Creating Secure Consumer..."
consumer = Kafkaesque::Consumer.new(consumer_config)

puts "[INFO] Security configurations registered successfully."
puts "[INFO] Ready to establish encrypted SCRAM connections with exponential backoff retries."
