require "./spec_helper"
require "./mock_broker"

describe "Kafkaesque Manual Partition Assignment" do
  it "successfully consumes from manually assigned partitions bypassing the coordinator" do
    broker = Kafkaesque::MockBroker.new
    metadata_calls = 0
    fetch_calls = 0
    heartbeat_calls = 0

    # 1. Mock Metadata Request (API KEY 3)
    broker.on_request(3_i16) do |decoder, version|
      metadata_calls += 1
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

      enc.write_array(["manual-topic"]) do |name|
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

    # 2. Mock Fetch Request (API KEY 1)
    broker.on_request(1_i16) do |decoder, version|
      fetch_calls += 1

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      enc.write_int32(0) # throttle_time_ms
      enc.write_array(["manual-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16)         # partition error code
          enc.write_int64(0_i64)         # high_watermark
          enc.write_int64(0_i64)         # last_stable_offset
          enc.write_array([] of Nil) { } # producer ids array (empty)

          # Serialize 1 RecordBatch
          record = Kafkaesque::Protocol::Record.new("key".to_slice, "val".to_slice, [] of Kafkaesque::Protocol::RecordHeader)
          record.offset = 0_i64

          batch = Kafkaesque::Protocol::RecordBatch.new([record])

          batch_io = IO::Memory.new
          batch.serialize(batch_io)

          enc.write_bytes(batch_io.to_slice)
        end
      end
      io
    end

    # 3. Mock Heartbeat / Join Group (API KEY 84)
    broker.on_request(84_i16) do |decoder, version|
      heartbeat_calls += 1
      io = IO::Memory.new
      io
    end

    begin
      config = Kafkaesque::Consumer::Config.new(["127.0.0.1:#{broker.port}"])
      consumer = Kafkaesque::Consumer.new(config)

      # Assign partition manually
      tp = Kafkaesque::TopicPartition.new("manual-topic", 0)
      consumer.assign(tp)

      consumer.manual_assignments.should_not be_nil
      consumer.manual_assignments.try(&.size).should eq(1)

      # Consume a single message
      spawn do
        consumer.each do |record|
          record.value.should eq("val")
          consumer.close
        end
      end

      # Wait for consumer to process
      sleep 200.milliseconds

      # Verify metrics
      fetch_calls.should be >= 1
      metadata_calls.should be >= 1
      heartbeat_calls.should eq(0) # Coordinator group loops must be bypassed!
    ensure
      broker.close
    end
  end
end
