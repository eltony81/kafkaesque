require "./spec_helper"
require "./mock_broker"

class Kafkaesque::Consumer
  def test_resolve_regex_topics(pattern : Regex)
    resolve_regex_topics(pattern)
  end
end

describe "Kafkaesque Advanced Features" do
  it "successfully executes regex subscription and discovers matching topics" do
    broker = Kafkaesque::MockBroker.new

    # Mock Metadata Request (API KEY 3)
    broker.on_request(3_i16) do |decoder, version|
      # Skip requested topics array
      decoder.read_array { decoder.read_string }

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      # 1. Brokers array: 1 broker (this mock broker itself)
      enc.write_array([nil]) do
        enc.write_int32(1)            # node_id
        enc.write_string("127.0.0.1") # host
        enc.write_int32(broker.port)  # port
        enc.write_string(nil)         # rack
      end

      # 2. Cluster ID
      enc.write_string("mock-cluster")
      # 3. Controller ID
      enc.write_int32(1)

      # 4. Topics array
      enc.write_array(["sensor-temperature", "sensor-humidity", "system-log"]) do |name|
        enc.write_int16(0_i16) # error_code
        enc.write_string(name) # name
        enc.write_int8(0_i8)   # is_internal (false)
        # Partitions array
        enc.write_array([nil]) do
          enc.write_int16(0_i16)           # error_code
          enc.write_int32(0)               # partition_index
          enc.write_int32(1)               # leader_id (broker 1)
          enc.write_array([] of Int32) { } # replicas
          enc.write_array([] of Int32) { } # isr
        end
      end

      io
    end

    begin
      config = Kafkaesque::Consumer::Config.new(["127.0.0.1:#{broker.port}"])
      consumer = Kafkaesque::Consumer.new(config)

      # Subscribe using Regex
      consumer.subscribe(/^sensor-.*$/)

      # Resolve matching topics
      consumer.subscription_pattern.should_not be_nil

      # We simulate what each does when starting:
      consumer.test_resolve_regex_topics(/^sensor-.*$/)
      consumer.@topics.should eq(["sensor-humidity", "sensor-temperature"])
    ensure
      broker.close
    end
  end

  it "successfully retries producing and fetches new leader when receiving LEADER_NOT_AVAILABLE" do
    broker = Kafkaesque::MockBroker.new

    metadata_calls = 0
    produce_calls = 0

    # Mock Metadata Request (API KEY 3)
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

      enc.write_array(["sensor-temp"]) do |name|
        enc.write_int16(0_i16)
        enc.write_string(name)
        enc.write_int8(0_i8)
        enc.write_array([nil]) do
          enc.write_int16(0_i16)
          enc.write_int32(0)
          enc.write_int32(1) # Leader is broker 1
          enc.write_array([] of Int32) { }
          enc.write_array([] of Int32) { }
        end
      end
      io
    end

    # Mock Produce Request (API KEY 0)
    broker.on_request(0_i16) do |decoder, version|
      produce_calls += 1
      # Decode standard ProduceRequest header details to skip to topic
      decoder.read_int16 # acks
      decoder.read_int32 # timeout

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      # Respond with error_code 5 (LEADER_NOT_AVAILABLE) on first attempt
      # format: topics array (size 1)
      enc.write_array(["sensor-temp"]) do |topic|
        enc.write_string(topic)
        # partitions array (size 1)
        enc.write_array([0]) do |part|
          enc.write_int32(part) # partition index
          if produce_calls == 1
            enc.write_int16(5_i16) # LEADER_NOT_AVAILABLE
          else
            enc.write_int16(0_i16) # Success
          end
          enc.write_int64(100_i64) # base_offset
          enc.write_int64(-1_i64)  # log_append_time
          enc.write_int64(0_i64)   # log start offset
        end
      end
      # throttle_time_ms
      enc.write_int32(0)
      io
    end

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      # Perform produce
      resp = client.produce("sensor-temp", "key", "val", partition: 0)

      # Check that it retried
      produce_calls.should eq(2)
      metadata_calls.should be >= 2 # 1 initial lookup + 1 recovery lookup
      resp.error_code.should eq(0)  # Eventually succeeded
    ensure
      broker.close
    end
  end

  it "respects configurable max_retries limit" do
    broker = Kafkaesque::MockBroker.new
    produce_calls = 0

    # Mock Metadata Request (API KEY 3)
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
      enc.write_array(["sensor-temp"]) do |name|
        enc.write_int16(0_i16)
        enc.write_string(name)
        enc.write_int8(0_i8)
        enc.write_array([nil]) do
          enc.write_int16(0_i16)
          enc.write_int32(0)
          enc.write_int32(1)
          enc.write_array([] of Int32) { }
          enc.write_array([] of Int32) { }
        end
      end
      io
    end

    # Mock Produce Request (API KEY 0)
    broker.on_request(0_i16) do |decoder, version|
      produce_calls += 1
      decoder.read_int16 # acks
      decoder.read_int32 # timeout

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_array(["sensor-temp"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(5_i16) # LEADER_NOT_AVAILABLE (keeps failing)
          enc.write_int64(100_i64)
          enc.write_int64(-1_i64)
          enc.write_int64(0_i64)
        end
      end
      enc.write_int32(0)
      io
    end

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port, max_retries: 2)
      client.connect

      # We expect it to try 2 times (initial + 1 retry) and then return the failed response
      resp = client.produce("sensor-temp", "key", "val", partition: 0)
      produce_calls.should eq(2)
      resp.error_code.should eq(5)
    ensure
      broker.close
    end
  end
end
