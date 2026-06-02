# This example demonstrates subscribing to Kafka topics dynamically using a Regex pattern.
# A background discovery loop automatically identifies newly created topics in the cluster
# matching the regular expression and subscribes the consumer to them.

require "../src/kafkaesque"
require "log"

# Setup logging to see metadata updates
Log.setup(:debug)

# Initialize configuration
config = Kafkaesque::Consumer::Config.new(["localhost:9092"]).tap do |cfg|
  cfg.set("group.id", "regex-consumer-example")
  cfg.set("initial_offset_smallest", "true")
end

consumer = Kafkaesque::Consumer.new(config)

# Subscribe to any topic starting with "sensor-" or "metrics-"
pattern = /^(sensor|metrics)-.*$/
puts "Subscribing to topics matching: #{pattern}"
consumer.subscribe(pattern)

# Setup termination signal handling
spawn do
  Process.on_terminate do
    puts "Shutdown requested..."
    consumer.close
    exit
  end
end

begin
  # Block and stream records sequentially as partitions match and rebalance
  consumer.each do |message|
    puts "Received on topic [#{message.topic}] partition [#{message.partition}] offset [#{message.offset}]"
    puts "  Key: #{message.key}"
    puts "  Value: #{message.value}"
  end
ensure
  consumer.close
end
