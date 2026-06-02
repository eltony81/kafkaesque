# This example demonstrates how to write local integration tests using Kafkaesque::MockBroker
# without running a real Kafka instance/container.

require "../src/kafkaesque"
require "../src/kafkaesque/mock_broker"

# Start mock broker on random local port
puts "Starting local MockBroker..."
broker = Kafkaesque::MockBroker.new
puts "MockBroker listening on 127.0.0.1:#{broker.port}"

# Mock Response for Metadata Request (API KEY 3)
broker.on_request(3_i16) do |decoder, version|
  # Skip requested topics array
  decoder.read_array { decoder.read_string }

  io = IO::Memory.new
  enc = Kafkaesque::Protocol::Encoder.new(io)

  # Brokers array
  enc.write_array([nil]) do
    enc.write_int32(1)              # node_id
    enc.write_string("127.0.0.1")    # host
    enc.write_int32(broker.port)     # port
    enc.write_string(nil)            # rack
  end
  # Cluster ID
  enc.write_string("mock-cluster")
  # Controller ID
  enc.write_int32(1)

  # Topics array
  enc.write_array(["test-topic"]) do |name|
    enc.write_int16(0_i16)        # error_code
    enc.write_string(name)        # name
    enc.write_int8(0_i8)          # is_internal
    # Partitions
    enc.write_array([nil]) do
      enc.write_int16(0_i16)      # error_code
      enc.write_int32(0)          # partition_index
      enc.write_int32(1)          # leader_id
      enc.write_array([] of Int32) { } # replicas
      enc.write_array([] of Int32) { } # isr
    end
  end
  io
end

# Mock Response for Produce Request (API KEY 0)
broker.on_request(0_i16) do |decoder, version|
  decoder.read_int16 # acks
  decoder.read_int32 # timeout

  io = IO::Memory.new
  enc = Kafkaesque::Protocol::Encoder.new(io)

  # Response outer topics array (size 1)
  enc.write_array(["test-topic"]) do |topic|
    enc.write_string(topic)
    # Partitions array (size 1)
    enc.write_array([0]) do |part|
      enc.write_int32(part)    # Partition index
      enc.write_int16(0_i16)   # Success error_code
      enc.write_int64(777_i64) # Committed base offset
      enc.write_int64(-1_i64)  # Log append time
      enc.write_int64(0_i64)   # Log start offset
    end
  end
  enc.write_int32(0) # throttle_time_ms
  io
end

begin
  puts "Initializing client connected to MockBroker..."
  client = Kafkaesque::Client.new("127.0.0.1", broker.port)
  client.connect

  puts "Sending produce request..."
  resp = client.produce("test-topic", "key", "val")
  
  if resp.error_code == 0
    puts "✅ Succeeded! Message written at offset: #{resp.base_offset}"
  else
    puts "❌ Failed with error code: #{resp.error_code}"
  end
ensure
  puts "Closing MockBroker..."
  broker.close
end
