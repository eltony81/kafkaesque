require "../src/kafkaesque"
require "../spec/mock_broker"
require "log"

Log.setup(:debug)

# 1. Spin up a local mock Kafka broker
broker = Kafkaesque::MockBroker.new

# Mock Metadata response
broker.on_request(3_i16) do |decoder, version|
  decoder.read_array { decoder.read_string }
  io = IO::Memory.new
  enc = Kafkaesque::Protocol::Encoder.new(io)
  enc.write_array([nil]) do
    enc.write_int32(1)
    enc.write_string("127.0.0.1")
    enc.write_int32(broker.port)
    enc.write_string(nil)
  end
  enc.write_string("mock-cluster")
  enc.write_int32(1)
  enc.write_array(["test-topic"]) do |name|
    enc.write_int16(0_i16)
    enc.write_string(name)
    enc.write_int8(0_i8)
    enc.write_array([nil]) do
      enc.write_int16(0_i16)
      enc.write_int32(0) # Partition 0
      enc.write_int32(1)
      enc.write_array([] of Int32) { }
      enc.write_array([] of Int32) { }
    end
  end
  io
end

# Mock Fetch response returning messages
broker.on_request(1_i16) do |decoder, version|
  io = IO::Memory.new
  enc = Kafkaesque::Protocol::Encoder.new(io)
  enc.write_int32(0) # throttle_time_ms
  enc.write_array(["test-topic"]) do |topic|
    enc.write_string(topic)
    enc.write_array([0]) do |part|
      enc.write_int32(part)
      enc.write_int16(0_i16) # partition error code
      enc.write_int64(0_i64) # high_watermark
      enc.write_int64(0_i64) # last_stable_offset
      enc.write_array([] of Nil) { } # producer ids array (empty)

      record = Kafkaesque::Protocol::Record.new("key".to_slice, "Hello, Kafkaesque Manual Assignment!".to_slice, [] of Kafkaesque::Protocol::RecordHeader)
      record.offset = 0_i64
      batch = Kafkaesque::Protocol::RecordBatch.new([record])

      batch_io = IO::Memory.new
      batch.serialize(batch_io)
      enc.write_bytes(batch_io.to_slice)
    end
  end
  io
end

puts "Mock Broker started on port #{broker.port}"

# 2. Configure Consumer
config = Kafkaesque::Consumer::Config.new(["127.0.0.1:#{broker.port}"])
consumer = Kafkaesque::Consumer.new(config)

# 3. Assign explicitly to partition 0 of topic "test-topic" (bypassing group coordinator)
tp = Kafkaesque::TopicPartition.new("test-topic", 0)
consumer.assign(tp)

puts "Manually assigned consumer to topic: #{tp.topic}, partition: #{tp.partition}"

# Spawn a termination fiber to stop after receiving a message
spawn do
  sleep 1.second
  puts "Stopping consumer..."
  consumer.close
  broker.close
end

# 4. Start consumption loop
consumer.each do |record|
  puts "Successfully received message: '#{record.value}'"
end

puts "Done!"
