require "../src/kafkaesque"
require "json"

# Configure a simple producer (PLAINTEXT connection)
config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  settings: {
    "enable.idempotence" => "true",
    "acks"               => "all",
    "linger.ms"          => "20",
    "batch.num.messages" => "1000",
  }
)

producer = Kafkaesque::Producer.new(config)

# Register message delivery confirmation callbacks
producer.on_deliver do |topic, partition, offset, exception|
  if exception
    puts "❌ Delivery failed on #{topic}:#{partition} - #{exception.message}"
  else
    puts "📤 Delivered message to #{topic}:#{partition} at offset #{offset}"
  end
end

begin
  topic = "telemetry-data"
  puts "🚀 Simple Producer started..."

  # Produce 10 complex telemetry events
  10.times do |i|
    payload = {
      "sensor_id" => "temp-sensor-1",
      "timestamp" => Time.utc.to_rfc3339,
      "reading"   => Random.rand(15.0..35.0).round(2),
      "index"     => i + 1,
    }.to_json

    key = "sensor_1"
    headers = {
      "correlation_id" => "msg-#{i + 1}",
      "client_id"      => "telemetry-prod",
    }

    # Queue message in the background batch accumulator
    producer.produce(topic, payload, key: key, headers: headers)
  end

  # Force-flush any remaining batched messages to the broker socket
  puts "⏳ Flushing remaining messages..."
  producer.flush(timeout_ms: 5000)
  puts "✅ Telemetry messages sent successfully!"
ensure
  # Cleanly shut down connections
  producer.close
end
