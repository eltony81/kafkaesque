# This example demonstrates KIP-714 (Client Metrics Receiver / Telemetry)
# showing how to subscribe and push client metrics directly to the broker.

require "../src/kafkaesque"
require "log"
require "uuid"

Log.setup(:debug)

puts "=== Kafkaesque KIP-714 Client Telemetry Demo ==="

# Initialize connection client
client = Kafkaesque::Client.new("localhost", 9092)

begin
  client.connect
  puts "Connected to broker successfully."

  # 1. Retrieve the metrics subscription configured by the broker
  puts "Negotiating telemetry subscription with the broker..."
  sub_resp = client.get_telemetry_subscription(
    requested_metrics: ["org.apache.kafka.client.producer.latency", "org.apache.kafka.client.consumer.poll"]
  )

  if sub_resp.error_code == 0
    puts "Subscription accepted!"
    puts "Subscription ID: #{sub_resp.subscription_id}"
    puts "Suggested push interval: #{sub_resp.push_interval_ms}ms"

    # Generate a faked UUID client instance ID (16 bytes)
    client_instance_id = UUID.random.bytes.to_slice

    # Simulated client metrics payload (e.g. OTLP protobuf or JSON formatted bytes)
    simulated_metrics = "metrics_data_payload_bytes".to_slice

    # 2. Push client telemetry to the broker receiver
    puts "Pushing client telemetry metrics to the broker..."
    push_resp = client.push_client_telemetry(
      subscription_id: sub_resp.subscription_id,
      client_instance_id: client_instance_id,
      metrics_data: simulated_metrics
    )

    if push_resp.error_code == 0
      puts "Telemetry metrics pushed successfully!"
    else
      puts "Telemetry push rejected with error code: #{push_resp.error_code}"
    end
  else
    puts "Telemetry subscription rejected with error code: #{sub_resp.error_code}"
  end
ensure
  client.close
end
