# This example demonstrates KIP-511 (Client Software Name/Version advertisement)
# and KIP-392 (Closest Replica routing based on rack configuration) in Kafkaesque.

require "../src/kafkaesque"
require "log"

# Enable debug logging so we can see API version handshakes and replica assignments
Log.setup(:debug)

puts "=== Kafkaesque KIP-511 & KIP-392 Demonstration ==="

# 1. Initialize client with a specific rack ID (KIP-392)
# Under the hood, this sets the preferred client_rack context.
config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  group_id: "kip-demo-group",
  client_rack: "us-east-1a" # Your local AWS availability zone / rack location
)

consumer = Kafkaesque::Consumer.new(config)

# 2. Start the consumer subscription
consumer.subscribe(["telemetry-data"])

# Under the hood, during consumer connection:
# - KIP-511 runs automatically. The client sends an ApiVersionsRequest v3,
#   identifying itself as "kafkaesque" with its current version.
# - The broker parses this client software information for cluster operators.
# - The client caches the broker's supported api key versions.

# For demonstration, let's explore what the client negotiated:
spawn do
  # Wait for consumer connection loop to start and query versions
  sleep 1.second

  if client = consumer.@client
    puts "\n--- [KIP-511] Negotiated Broker API Versions ---"
    client.api_versions.each do |info|
      # E.g. API key 18 (ApiVersions), key 0 (Produce), key 1 (Fetch)
      puts "API Key: #{info.api_key} | Supported Version: v#{info.min_version} to v#{info.max_version}"
    end

    # Check current rack routing configuration
    puts "\n--- [KIP-392] Closest Replica Configuration ---"
    puts "Client configured rack location: #{client.client_rack}"

    # When fetching, the client will lookup the partition replicas.
    # If any replica resides on a broker with the rack matching "us-east-1a",
    # the client will route read requests to that follower replica instead of the leader!
  end
end

puts "\nStarting consumer loop... Press Ctrl+C to stop."
begin
  consumer.each do |message|
    puts "Received: #{message.value} from partition #{message.partition} @ offset #{message.offset}"
  end
ensure
  consumer.close
end
