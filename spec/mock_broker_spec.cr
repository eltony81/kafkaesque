require "./spec_helper"
require "../src/kafkaesque/mock_broker"

# Coverage for MockBroker's built-in stateful defaults (#register_topic) —
# the fallback Metadata/Produce/Fetch handling used when a test doesn't
# register its own on_request handler for those API keys.
describe "Kafkaesque::MockBroker stateful defaults" do
  it "round-trips produced records through the default in-memory log via Client#fetch" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("auto-topic", partitions: 2)

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      produce_resp = client.produce("auto-topic", "k1", "v1", partition: 0)
      produce_resp.error_code.should eq(0)
      produce_resp.base_offset.should eq(0)

      produce_resp2 = client.produce("auto-topic", "k2", "v2", partition: 0)
      produce_resp2.base_offset.should eq(1)

      # A different partition gets its own independent log.
      client.produce("auto-topic", "k3", "v3", partition: 1)

      fetch_resp = client.fetch("auto-topic", partition: 0, fetch_offset: 0_i64)
      fetch_resp.error_code.should eq(0)
      fetch_resp.records.size.should eq(2)
      fetch_resp.records[0].value.to_s.should eq("v1")
      fetch_resp.records[0].offset.should eq(0)
      fetch_resp.records[1].value.to_s.should eq("v2")
      fetch_resp.records[1].offset.should eq(1)

      # Fetching from an offset partway through returns only the remainder.
      partial = client.fetch("auto-topic", partition: 0, fetch_offset: 1_i64)
      partial.records.size.should eq(1)
      partial.records[0].value.to_s.should eq("v2")

      partition1 = client.fetch("auto-topic", partition: 1, fetch_offset: 0_i64)
      partition1.records.size.should eq(1)
      partition1.records[0].value.to_s.should eq("v3")
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "returns UNKNOWN_TOPIC_OR_PARTITION for topics that were never registered" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("known-topic")

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      resp = client.produce("unknown-topic", "k", "v")
      resp.error_code.should eq(3) # UNKNOWN_TOPIC_OR_PARTITION
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "still allows a custom on_request handler to override the default for a given API key" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("auto-topic")
    custom_handler_called = false

    broker.on_request(0_i16) do |decoder, version|
      custom_handler_called = true
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_array(["auto-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16)
          enc.write_int64(999_i64)
          enc.write_int64(-1_i64)
          enc.write_int64(0_i64)
        end
      end
      enc.write_int32(0)
      io
    end

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      resp = client.produce("auto-topic", "k", "v")
      custom_handler_called.should be_true
      resp.base_offset.should eq(999)
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "serves independent logs for multiple registered topics from one Metadata response" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("topic-a", partitions: 1)
    broker.register_topic("topic-b", partitions: 3)

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      client.produce("topic-a", "k", "a-val")
      client.produce("topic-b", "k", "b-val", partition: 2)

      client.partitions_count("topic-a").should eq(1)
      client.partitions_count("topic-b").should eq(3)

      a = client.fetch("topic-a", partition: 0, fetch_offset: 0_i64)
      a.records.first.value.to_s.should eq("a-val")

      b = client.fetch("topic-b", partition: 2, fetch_offset: 0_i64)
      b.records.first.value.to_s.should eq("b-val")
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "round-trips a full Producer/Consumer flow through register_topic (no custom handlers at all)" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("app-events", partitions: 1)

    begin
      producer = Kafkaesque::Producer.new(
        Kafkaesque::Producer::Config.new(bootstrap_servers: ["127.0.0.1:#{broker.port}"])
      )
      producer.produce("app-events", "hello-world", key: "k1")
      producer.flush

      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect
      fetched = client.fetch("app-events", partition: 0, fetch_offset: 0_i64)
      fetched.records.first.value.to_s.should eq("hello-world")
    ensure
      producer.try(&.close) rescue nil
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "delays every response by #latency_ms" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("slow-topic")
    broker.latency_ms = 100

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      started = Time.instant
      client.produce("slow-topic", "k", "v")
      elapsed = Time.instant - started
      elapsed.total_milliseconds.should be >= 100
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "drops the connection after #drop_after_requests requests, surfacing as an IO error" do
    broker = Kafkaesque::MockBroker.new
    broker.register_topic("flaky-topic")
    broker.drop_after_requests = 1

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port, max_retries: 1)
      client.connect

      expect_raises(IO::Error) do
        client.produce("flaky-topic", "k", "v")
      end
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end

  it "persists OffsetCommit state and returns it from a subsequent OffsetFetch" do
    broker = Kafkaesque::MockBroker.new

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      client.offset_commit(group_id: "g1", generation_id: 1, member_id: "m1", topic: "t", partition: 0, offset: 42_i64)
      resp = client.offset_fetch(group_id: "g1", topic: "t", partition: 0)
      resp.committed_offset.should eq(42_i64)
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end
end

describe "Kafkaesque::Protocol::OffsetForLeaderEpochRequest/Response (KIP-320)" do
  it "serializes and deserializes correctly" do
    req = Kafkaesque::Protocol::OffsetForLeaderEpochRequest.new("my-topic", 2, current_leader_epoch: 5, leader_epoch: 3)

    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)
    req.serialize(encoder)

    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)
    decoder.read_array do
      decoder.read_string.should eq("my-topic")
      decoder.read_array do
        decoder.read_int32.should eq(2) # partition
        decoder.read_int32.should eq(5) # current_leader_epoch
        decoder.read_int32.should eq(3) # leader_epoch
      end
    end
  end

  it "deserializes a response with a detected truncation" do
    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    enc.write_int32(0) # throttle_time_ms
    enc.write_array(["my-topic"]) do |topic|
      enc.write_string(topic)
      enc.write_array([0]) do |_|
        enc.write_int16(0_i16) # error_code
        enc.write_int32(0)     # partition
        enc.write_int32(7)     # leader_epoch
        enc.write_int64(3_i64) # end_offset
      end
    end

    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)
    resp = Kafkaesque::Protocol::OffsetForLeaderEpochResponse.deserialize(decoder)
    resp.error_code.should eq(0)
    resp.partition.should eq(0)
    resp.leader_epoch.should eq(7)
    resp.end_offset.should eq(3_i64)
  end
end

describe "Kafkaesque::Client#fetch KIP-320 truncation detection end-to-end (MockBroker)" do
  it "rewinds the fetch offset via OffsetForLeaderEpoch after a leader-epoch change reveals truncation" do
    broker = Kafkaesque::MockBroker.new
    topic_id = Bytes.new(16, 1_u8)
    metadata_calls = 0
    fetch_calls = 0
    ole_calls = 0
    observed_fetch_offset = -1_i64
    observed_current_leader_epoch = -1
    observed_ole_current_epoch = -1
    observed_ole_leader_epoch = -1

    broker.on_request(3_i16) do |decoder, version| # Metadata (v12)
      metadata_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      write_mock_metadata_prefix(enc, 1, "127.0.0.1", broker.port)
      # First call reports leader_epoch=1; after the simulated leader change,
      # subsequent calls report leader_epoch=2.
      epoch = metadata_calls == 1 ? 1 : 2
      enc.write_compact_array(["trunc-topic"]) do |name|
        write_mock_metadata_topic(enc, name, [{0, 1}], leader_epoch: epoch)
      end
      enc.write_tag_buffer
      io
    end

    broker.on_request(1_i16) do |decoder, version| # Fetch (v11)
      fetch_calls += 1
      decoder.read_int32 # replica_id
      decoder.read_int32 # max_wait_ms
      decoder.read_int32 # min_bytes
      decoder.read_int32 # max_bytes
      decoder.read_int8  # isolation_level
      decoder.read_int32 # session_id
      decoder.read_int32 # session_epoch
      decoder.read_array do
        decoder.read_string # topic
        decoder.read_array do
          decoder.read_int32 # partition
          observed_current_leader_epoch = decoder.read_int32
          observed_fetch_offset = decoder.read_int64
          decoder.read_int64 # log_start_offset
          decoder.read_int32 # partition_max_bytes
        end
      end

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # top-level error_code
      enc.write_int32(0)     # session_id
      enc.write_array(["trunc-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          # First attempt (still fenced under the stale epoch=1) is rejected;
          # the retry (after rewinding + epoch=2) succeeds.
          enc.write_int16(fetch_calls == 1 ? 6_i16 : 0_i16) # NOT_LEADER_OR_FOLLOWER, then OK
          enc.write_int64(0_i64)                            # high_watermark
          enc.write_int64(0_i64)                            # last_stable_offset
          enc.write_int64(0_i64)                            # log_start_offset
          enc.write_array([] of Int32) { }
          enc.write_int32(-1) # preferred_read_replica
          enc.write_bytes(Bytes.empty)
        end
      end
      io
    end

    broker.on_request(23_i16) do |decoder, version| # OffsetForLeaderEpoch
      ole_calls += 1
      decoder.read_array do
        decoder.read_string # topic
        decoder.read_array do
          decoder.read_int32 # partition
          observed_ole_current_epoch = decoder.read_int32
          observed_ole_leader_epoch = decoder.read_int32
        end
      end

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0) # throttle_time_ms
      enc.write_array(["trunc-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |_|
          enc.write_int16(0_i16) # error_code
          enc.write_int32(0)     # partition
          enc.write_int32(2)     # leader_epoch
          enc.write_int64(3_i64) # end_offset: broker only has data up to offset 3 under the old epoch
        end
      end
      io
    end

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      # Ask for offset 10, but the (simulated) unclean leader election means
      # the new leader's log for the old epoch only goes up to offset 3.
      resp = client.fetch("trunc-topic", partition: 0, fetch_offset: 10_i64)

      resp.error_code.should eq(0)
      metadata_calls.should be >= 2
      fetch_calls.should eq(2)
      ole_calls.should eq(1)

      observed_ole_current_epoch.should eq(2) # fenced with the NEW epoch
      observed_ole_leader_epoch.should eq(1)  # asking about the OLD epoch we were fetching under

      # The retried Fetch must have rewound to the truncation-safe offset (3),
      # not blindly retried at the original (now-invalid) offset (10), and
      # must fence with the new leader epoch (2).
      observed_fetch_offset.should eq(3_i64)
      observed_current_leader_epoch.should eq(2)
    ensure
      client.try(&.close) rescue nil
      broker.close
    end
  end
end
