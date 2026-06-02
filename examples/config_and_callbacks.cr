require "../src/kafkaesque"
require "json"

# This example demonstrates loading configurations from a YAML file
# and registering performance/lifecycle callbacks on both Producer and Consumer.

config_file = "examples/config.yml"
puts "📄 Loading configurations from #{config_file}..."

# =====================================================================
# 1. PRODUCER WITH CALLBACKS
# =====================================================================
producer_config = Kafkaesque::ConfigLoader.load_producer_config(config_file)
producer = Kafkaesque::Producer.new(producer_config)

# Callback A: Triggered when a message is successfully delivered or fails
producer.on_deliver do |topic, partition, offset, exception|
  if exception
    puts "❌ [Producer Callback] Delivery failed: #{exception.message}"
  else
    puts "✅ [Producer Callback] Confirmed delivery on #{topic}:#{partition} at offset #{offset}"
  end
end

# Callback B: Reports client performance stats periodically
producer.on_stats do |stats_json|
  puts "📊 [Producer Stats] #{stats_json}"
end

# =====================================================================
# 2. CONSUMER WITH CALLBACKS
# =====================================================================
consumer_config = Kafkaesque::ConfigLoader.load_consumer_config(config_file)
consumer = Kafkaesque::Consumer.new(consumer_config)

# Callback C: Triggered when broker coordinator assigns partition ownership
consumer.on_partitions_assigned do |partitions|
  puts "📥 [Consumer Callback] Assigned partitions: #{partitions}"
end

# Callback D: Triggered when partition ownership is revoked
consumer.on_partitions_revoked do |partitions|
  puts "📤 [Consumer Callback] Revoked partitions: #{partitions}"
end

# Handle shutdown signals cleanly
Process.on_terminate do
  puts "\n🛑 Stopping services..."
  producer.close
  consumer.close
  exit
end

# Run the producer demonstration
begin
  puts "🚀 Producing test messages..."
  producer.produce("telemetry-data", "Sensor event payload", key: "sensor_x")
  producer.flush
ensure
  producer.close
end

# Run the consumer demonstration (subscribing to listen)
consumer.subscribe(["telemetry-data"])
begin
  puts "🚀 Consumer listening for events..."
  # Streaming loop
  # consumer.each do |message|
  #   puts message.value
  # end
ensure
  consumer.close
end
