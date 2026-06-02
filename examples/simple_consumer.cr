require "../src/kafkaesque"
require "log"

# Enable detailed logs to monitor partition rebalances and heartbeat states
Log.setup(:debug)

# Configure a simple consumer supporting KIP-848 server-side assignments
config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  group_id: "telemetry-consumers",
  initial_offset_smallest: true
)

consumer = Kafkaesque::Consumer.new(config)

# Subscribe to topic partition boundaries
consumer.subscribe(["telemetry-data"])

# Register rebalance event callbacks
consumer.on_partitions_assigned do |partitions|
  puts "[ASSIGNED] Broker assigned partitions ownership: #{partitions}"
end

consumer.on_partitions_revoked do |partitions|
  puts "[REVOKED] Partition ownership revoked by broker: #{partitions}"
end

# Handle termination signals cleanly
Process.on_terminate do
  puts "\n[STOP] Shutdown requested. Leaving consumer group..."
  consumer.close
  exit
end

begin
  puts "[START] Consumer started. Streaming telemetry messages..."
  # Blocks the loop and yields incoming records sequentially
  consumer.each do |message|
    puts "Offset: #{message.offset} | Key: #{message.key} | Payload: #{message.value}"
    puts "Headers: #{message.headers.map { |h| "#{h.key}=#{h.value}" }}"
  end
ensure
  consumer.close
end
