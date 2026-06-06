require "../src/kafkaesque"
require "log"
require "uuid"

Log.setup(:debug)

puts "=== Live Integration Test (SASL OAuthBearer) for KIP-511, KIP-392, KIP-932 & KIP-714 ==="

# Initialize Consumer Config with SASL settings matching kafkaclitest config.yml
config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9093"],
  group_id: "mio-gruppo-crystal-kip848-test",
  settings: {
    "sasl.oauthbearer.token.endpoint.url" => "http://localhost:8080/realms/kafka-auth/protocol/openid-connect/token",
    "sasl.oauthbearer.client.id"          => "kafka-client",
    "sasl.oauthbearer.client.secret"      => "kafka-secret"
  }
)

consumer = Kafkaesque::Consumer.new(config)
consumer.subscribe(["telemetry-data"])

# Start consumer initialization in a background fiber or run directly
puts "Connecting consumer..."
begin
  # Trigger coordinator resolution which connects to the bootstrap server and runs ApiVersions/SASL
  attempts = 0
  client = Kafkaesque::Client.connect_first(
    servers: config.bootstrap_servers,
    sasl_token: config.sasl_token,
    client_id: "kafkaesque-integration-client",
    oauth_token_provider: config.oauth_token_provider,
    max_retries: 3
  )

  puts "\n--- [KIP-511] ApiVersions Handshake ---"
  puts "ApiVersions successfully queried!"
  puts "Negotiated ApiVersions count: #{client.api_versions.size}"
  client.api_versions.first(5).each do |info|
    puts "  API Key: #{info.api_key} (v#{info.min_version} to v#{info.max_version})"
  end

  # Test KIP-714
  puts "\n--- [KIP-714] Client Telemetry ---"
  begin
    has_telemetry = client.api_versions.any? { |info| info.api_key == 71 }
    if has_telemetry
      sub_resp = client.get_telemetry_subscription(
        client_instance_id: Bytes.new(16)
      )
      if sub_resp.error_code == 0
        puts "Telemetry subscription fetched! ID: #{sub_resp.subscription_id}"
        push_resp = client.push_client_telemetry(
          subscription_id: sub_resp.subscription_id,
          client_instance_id: sub_resp.client_instance_id,
          metrics_data: "metrics_payload".to_slice
        )
        puts "Telemetry push returned error code: #{push_resp.error_code}"
      else
        puts "Telemetry subscription returned error code: #{sub_resp.error_code}"
      end
    else
      puts "Broker does not advertise KIP-714 Telemetry support (API Key 71)."
    end
  rescue ex
    puts "Telemetry test failed: #{ex.message}"
  end

  # Test KIP-392
  puts "\n--- [KIP-392] Closest Replica Routing ---"
  client.client_rack = "rack-a"
  begin
    metadata = client.fetch_metadata
    puts "Cluster topology and broker racks:"
    metadata.brokers.each do |b|
      puts "  Node: #{b.node_id} | Host: #{b.host} | Port: #{b.port} | Rack: #{b.rack}"
    end
  rescue ex
    puts "Topology metadata fetch failed: #{ex.message}"
  ensure
    client.close
  end

rescue ex
  puts "Failed to connect to cluster: #{ex.message}"
end

puts "\n=== Live Integration Test Completed ==="
