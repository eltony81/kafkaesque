# This example demonstrates how to write local integration tests using
# Kafkaesque::MockBroker without running a real Kafka instance/container.
#
# See examples/mock_testing_advanced.cr for custom on_request handlers and
# failure/latency simulation, once you outgrow the stateful defaults shown
# here.

require "../src/kafkaesque"
require "../src/kafkaesque/mock_broker"

puts "Starting local MockBroker..."
broker = Kafkaesque::MockBroker.new
puts "MockBroker listening on 127.0.0.1:#{broker.port}"

# register_topic backs Metadata/Produce/Fetch with a real in-memory log per
# partition — no need to hand-encode responses for the common case.
broker.register_topic("test-topic", partitions: 2)

begin
  puts "Initializing client connected to MockBroker..."
  client = Kafkaesque::Client.new("127.0.0.1", broker.port)
  client.connect

  puts "Sending produce request..."
  resp = client.produce("test-topic", "key", "val")

  if resp.error_code == 0
    puts "✅ Produce succeeded! Message written at offset: #{resp.base_offset}"
  else
    puts "❌ Produce failed with error code: #{resp.error_code}"
  end

  puts "Fetching it back..."
  fetched = client.fetch("test-topic", partition: 0, fetch_offset: 0_i64)
  record = fetched.records.first?
  if record
    puts "✅ Fetch succeeded! value=#{record.value} offset=#{record.offset}"
  else
    puts "❌ Fetch returned no records"
  end

  puts "Producing to unregistered partition 5..."
  bad_resp = client.produce("test-topic", "key", "val", partition: 5)
  puts "  error_code=#{bad_resp.error_code} (3 = UNKNOWN_TOPIC_OR_PARTITION, as expected)"
ensure
  client.try(&.close)
  puts "Closing MockBroker..."
  broker.close
end
