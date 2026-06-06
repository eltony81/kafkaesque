# This example demonstrates KIP-932 (Queues for Kafka / Share Groups)
# where multiple consumers process messages dynamically using queue-like semantics.

require "../src/kafkaesque"
require "log"

Log.setup(:debug)

puts "=== Kafkaesque KIP-932 Share Group Consumer ==="

config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  settings: {
    "group.id"   => "my-share-group", # Share group identifier
    "group.type" => "share",          # Type configured as a share group
  }
)

consumer = Kafkaesque::Consumer.new(config)
consumer.subscribe(["orders-topic"])

# Start consuming from the share group.
# In a share group, records are distributed dynamically amongst active members,
# and each message is individually acknowledged (ACKed).
puts "Starting Share Group consumer... Press Ctrl+C to stop."
begin
  consumer.share_each do |message|
    puts "Processing message: #{message.value} (Offset: #{message.offset})"
    # Under the hood, share_each automatically calls `share_acknowledge`
    # for each message processed, marking it as successfully handled in the queue.
  end
ensure
  consumer.close
end
