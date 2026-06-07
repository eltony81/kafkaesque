require "../src/kafkaesque"
require "file_utils"
require "json"

# ==============================================================================
# Examples: Enterprise Observability & Advanced Security Features
# This file demonstrates configuring:
# 1. Prometheus Metrics Exporter & Embedded HTTP Server
# 2. W3C Trace Context Propagation via Record Interceptors
# 3. Dynamic mTLS Certificate Reloading
# 4. Custom corporate SASL Authentication extension hooks
# ==============================================================================

# Create a temporary directory to host mock certificate files for mTLS reload demo
temp_dir = "./scratch/enterprise_demo_certs"
Dir.mkdir_p(temp_dir)
cert_file = File.join(temp_dir, "client.crt")
key_file = File.join(temp_dir, "client.key")

# Generate mock certificates using OpenSSL CLI
puts "[Demo Setup] Generating temporary self-signed TLS certificates..."
system("openssl req -x509 -newkey rsa:2048 -keyout #{key_file} -out #{cert_file} -days 1 -nodes -subj \"/CN=localhost\" > /dev/null 2>&1")

# ------------------------------------------------------------------------------
# 1. Register a Custom Corporate SASL Mechanism
# ------------------------------------------------------------------------------
# Enterprise environments often require custom authentication wrappers (e.g. Kerberos stub, custom token).
# Register custom hooks that can read parameters from settings and authenticate connection sockets.
Kafkaesque::Client.register_sasl_mechanism("ENTERPRISE_CUSTOM_AUTH") do |conn, settings|
  puts "[SASL] Invoked custom corporate authenticator registry mechanism"
  puts " - Custom authentication server: #{settings["custom.auth.server"]?}"
  # Authenticate connection socket using your enterprise protocols here...
end

# ------------------------------------------------------------------------------
# 2. Configure Enterprise Client (mTLS Cert Reloading & Embedded Prometheus Metrics Server)
# ------------------------------------------------------------------------------
# Set up a client that dynamically reloads certificates when they change on disk,
# starts an embedded HTTP Prometheus server, and uses our custom SASL mechanism.
config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["127.0.0.1:9092"],
  settings: {
    # Custom SASL Authentication
    "sasl.mechanism"     => "ENTERPRISE_CUSTOM_AUTH",
    "custom.auth.server" => "https://auth.corp.internal",

    # Dynamic mTLS Cert Reloading
    "security.protocol"                     => "SSL",
    "ssl.keystore.location"                 => cert_file,
    "ssl.keystore.key.location"             => key_file,
    "ssl.keystore.reload.interval.ms"       => "100", # periodically check files every 100ms
    "ssl.endpoint.identification.algorithm" => "none",
    "ssl.verify.peer"                       => "false",

    # Embedded Prometheus Metrics HTTP Exporter
    "metrics.prometheus.port" => "19090", # Serves metrics at http://localhost:19090/metrics
  }
)

# Initialize producer. Since bootstrap connection might fail if no broker runs on 9092, we rescue connection errors.
# In a real setup, a connection to a running Kafka cluster is established.
producer = nil
begin
  producer = Kafkaesque::Producer.new(config)
rescue ex
  puts "[Notice] Could not connect to Kafka Broker: #{ex.message}. Continuing with offline observability demonstration."
end

# ------------------------------------------------------------------------------
# 3. Tracing Interceptors (OpenTelemetry / W3C Trace Context)
# ------------------------------------------------------------------------------
# Register a send interceptor on the producer (if successfully initialized).
if p = producer
  p.on_send do |record|
    # Inject W3C traceparent (TraceID: 4bf92f3577b34da6a3ce929d0e0e4736, SpanID: 00f067aa0ba902b7)
    Kafkaesque::Tracing.inject_trace_context(
      headers: record.headers,
      trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
      span_id: "00f067aa0ba902b7",
      sampled: true
    )
    puts "[Tracing] Injected W3C trace context into record: #{record.key || record.value}"
    record
  end
end

# Register a consume interceptor on the consumer.
# It automatically extracts W3C context headers from records when they are consumed.
consumer_config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["127.0.0.1:9092"],
  group_id: "enterprise-processing-group"
)
consumer = Kafkaesque::Consumer.new(consumer_config)

consumer.on_consume do |record|
  if trace_info = Kafkaesque::Tracing.extract_trace_context(record.headers)
    puts "[Tracing] Extracted W3C trace context from consumed record:"
    puts " - Trace ID: #{trace_info[:trace_id]}"
    puts " - Span ID:  #{trace_info[:span_id]}"
    puts " - Sampled:  #{trace_info[:sampled]}"
  else
    puts "[Tracing] No W3C trace context headers found on consumed record."
  end
end

puts "🚀 Enterprise client initialized successfully!"
puts " - Prometheus metrics server is listening on port: #{config.settings.try(&.[]?("metrics.prometheus.port"))}"
puts " - Dynamic mTLS Key Reloading is active with file monitor."

# Simulate certificate file modification to trigger reload
puts "[Demo] Modifying certificate file to trigger a reload..."
sleep 200.milliseconds
system("openssl req -x509 -newkey rsa:2048 -keyout #{key_file} -out #{cert_file} -days 1 -nodes -subj \"/CN=localhost-updated\" > /dev/null 2>&1")
sleep 200.milliseconds

# Cleanly shutdown and close clients (which stops metrics server and reload loops)
producer.try &.close
consumer.close
FileUtils.rm_rf(temp_dir) rescue nil
puts "[Demo] Cleaned up temporary certificates."
