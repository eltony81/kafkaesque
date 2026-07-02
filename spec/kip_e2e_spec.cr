require "./spec_helper"
require "../src/kafkaesque/mock_broker"

# End-to-end (MockBroker wire round-trip, not just in-memory struct
# serialization) coverage for the KIPs advertised in the readme.

describe "KIP-848 ConsumerGroupHeartbeat end-to-end (MockBroker)" do
  it "joins the group, gets a partition assignment, and streams a fetched record through Consumer#each" do
    broker = Kafkaesque::MockBroker.new
    topic_id = Bytes.new(16, 4_u8)
    heartbeat_calls = 0
    fetch_calls = 0
    metadata_calls = 0

    broker.on_request(10_i16) do |decoder, version| # FindCoordinator
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      enc.write_string(nil)  # error_message
      enc.write_int32(1)     # coordinator node_id
      enc.write_string("127.0.0.1")
      enc.write_int32(broker.port)
      io
    end

    broker.on_request(3_i16) do |decoder, version| # Metadata (v12)
      metadata_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      write_mock_metadata_prefix(enc, 1, "127.0.0.1", broker.port)
      enc.write_compact_array(["e2e-topic"]) do |name|
        write_mock_metadata_topic(enc, name, [{0, 1}])
      end
      enc.write_tag_buffer
      io
    end

    broker.on_request(68_i16) do |decoder, version| # ConsumerGroupHeartbeat (KIP-848)
      heartbeat_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)            # throttle_time_ms
      enc.write_int16(0_i16)        # error_code
      enc.write_compact_string(nil) # error_message
      enc.write_compact_string("member-e2e")
      enc.write_int32(1)     # member_epoch
      enc.write_int32(30000) # heartbeat_interval_ms (large: keep the bg loop quiet for the test)
      enc.write_int8(1_i8)   # has_assignment: present
      enc.write_compact_array([topic_id]) do |uuid|
        enc.io.write(uuid)
        enc.write_compact_array([0]) { |p| enc.write_int32(p) }
        enc.write_tag_buffer
      end
      enc.write_tag_buffer # Assignment tag buffer
      enc.write_tag_buffer # Response tag buffer
      io
    end

    broker.on_request(1_i16) do |decoder, version| # Fetch (v11)
      fetch_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # top-level error_code (v7+)
      enc.write_int32(0)     # session_id (v7+)
      enc.write_array(["e2e-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16)         # partition error code
          enc.write_int64(0_i64)         # high_watermark
          enc.write_int64(0_i64)         # last_stable_offset
          enc.write_int64(-1_i64)        # log_start_offset
          enc.write_array([] of Nil) { } # aborted_transactions
          enc.write_int32(-1)            # preferred_read_replica
          if fetch_calls == 1
            record = Kafkaesque::Protocol::Record.new("k".to_slice, "e2e-value".to_slice, [] of Kafkaesque::Protocol::RecordHeader)
            record.offset = 0_i64
            batch = Kafkaesque::Protocol::RecordBatch.new([record])
            batch_io = IO::Memory.new
            batch.serialize(batch_io)
            enc.write_bytes(batch_io.to_slice)
          else
            enc.write_bytes(Bytes.empty)
          end
        end
      end
      io
    end

    begin
      config = Kafkaesque::Consumer::Config.new(
        ["127.0.0.1:#{broker.port}"],
        group_id: "e2e-group",
        settings: {"enable.auto.commit" => "false"}
      )
      consumer = Kafkaesque::Consumer.new(config)
      consumer.subscribe(["e2e-topic"])

      received = ""
      spawn do
        consumer.each do |record|
          received = record.value.to_s
          consumer.close
        end
      end

      sleep 300.milliseconds

      received.should eq("e2e-value")
      heartbeat_calls.should be >= 1
      fetch_calls.should be >= 1
      metadata_calls.should be >= 1
    ensure
      broker.close
    end
  end
end

describe "Classic consumer group protocol fallback (JoinGroup/SyncGroup, MockBroker)" do
  it "falls back from KIP-848 to JoinGroup/SyncGroup when the broker returns UNSUPPORTED_VERSION, and streams a fetched record" do
    broker = Kafkaesque::MockBroker.new
    heartbeat_calls = 0
    join_calls = 0
    sync_calls = 0
    fetch_calls = 0
    observed_subscribed_topics = [] of String

    broker.on_request(10_i16) do |decoder, version| # FindCoordinator
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      enc.write_string(nil)  # error_message
      enc.write_int32(1)     # coordinator node_id
      enc.write_string("127.0.0.1")
      enc.write_int32(broker.port)
      io
    end

    broker.on_request(3_i16) do |decoder, version| # Metadata (v12)
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      write_mock_metadata_prefix(enc, 1, "127.0.0.1", broker.port)
      enc.write_compact_array(["classic-topic"]) do |name|
        write_mock_metadata_topic(enc, name, [{0, 1}])
      end
      enc.write_tag_buffer
      io
    end

    broker.on_request(68_i16) do |decoder, version| # ConsumerGroupHeartbeat — always unsupported here
      heartbeat_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)                                # throttle_time_ms
      enc.write_int16(35_i16)                           # error_code: UNSUPPORTED_VERSION
      enc.write_compact_string("classic protocol only") # error_message
      enc.write_compact_string(nil)                     # member_id
      enc.write_int32(-1)                               # member_epoch
      enc.write_int32(0)                                # heartbeat_interval_ms
      enc.write_int8(0_i8)                              # has_assignment: absent
      enc.write_tag_buffer                              # Response tag buffer
      io
    end

    broker.on_request(11_i16) do |decoder, version| # JoinGroup (v0)
      join_calls += 1
      received_group_id = decoder.read_string.to_s
      decoder.read_int32 # session_timeout_ms
      received_member_id = decoder.read_string.to_s
      received_protocol_type = decoder.read_string.to_s
      protocols = decoder.read_array do
        name = decoder.read_string.to_s
        metadata = decoder.read_bytes || Bytes.empty
        {name, metadata}
      end.not_nil!

      sub = Kafkaesque::Protocol::ConsumerProtocolSubscription.deserialize(protocols.first[1])
      observed_subscribed_topics = sub.topics

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      # v0 response: no throttle_time_ms field
      enc.write_int16(0_i16)               # error_code
      enc.write_int32(1)                   # generation_id
      enc.write_string("range")            # protocol_name
      enc.write_string(received_member_id) # leader_id — sole member, so we're the leader
      enc.write_string(received_member_id) # member_id
      enc.write_array([{received_member_id, protocols.first[1]}]) do |(mid, metadata)|
        enc.write_string(mid)
        enc.write_bytes(metadata)
      end
      io
    end

    broker.on_request(14_i16) do |decoder, version| # SyncGroup (v1)
      sync_calls += 1
      decoder.read_string # group_id
      decoder.read_int32  # generation_id
      member_id = decoder.read_string.to_s
      assignments = decoder.read_array do
        mid = decoder.read_string.to_s
        bytes = decoder.read_bytes || Bytes.empty
        {mid, bytes}
      end.not_nil!
      my_assignment = assignments.find { |(mid, _)| mid == member_id }.try(&.[1]) || Bytes.empty

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      enc.write_bytes(my_assignment)
      io
    end

    broker.on_request(12_i16) do |decoder, version| # Heartbeat (classic) — steady-state OK
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      io
    end

    broker.on_request(1_i16) do |decoder, version| # Fetch (v11)
      fetch_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # top-level error_code (v7+)
      enc.write_int32(0)     # session_id (v7+)
      enc.write_array(["classic-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16)  # partition error code
          enc.write_int64(0_i64)  # high_watermark
          enc.write_int64(0_i64)  # last_stable_offset
          enc.write_int64(-1_i64) # log_start_offset
          enc.write_array([] of Nil) { }
          enc.write_int32(-1) # preferred_read_replica
          if fetch_calls == 1
            record = Kafkaesque::Protocol::Record.new("k".to_slice, "classic-value".to_slice, [] of Kafkaesque::Protocol::RecordHeader)
            record.offset = 0_i64
            batch = Kafkaesque::Protocol::RecordBatch.new([record])
            batch_io = IO::Memory.new
            batch.serialize(batch_io)
            enc.write_bytes(batch_io.to_slice)
          else
            enc.write_bytes(Bytes.empty)
          end
        end
      end
      io
    end

    begin
      config = Kafkaesque::Consumer::Config.new(
        ["127.0.0.1:#{broker.port}"],
        group_id: "classic-group",
        settings: {"enable.auto.commit" => "false"}
      )
      consumer = Kafkaesque::Consumer.new(config)
      consumer.subscribe(["classic-topic"])

      received = ""
      spawn do
        consumer.each do |record|
          received = record.value.to_s
          consumer.close
        end
      end

      sleep 300.milliseconds

      received.should eq("classic-value")
      heartbeat_calls.should be >= 1 # attempted KIP-848 first
      join_calls.should eq(1)
      sync_calls.should eq(1)
      fetch_calls.should be >= 1
      observed_subscribed_topics.should eq(["classic-topic"])
    ensure
      broker.close
    end
  end
end

describe "KIP-714 Client Telemetry end-to-end (MockBroker)" do
  it "round-trips GetTelemetrySubscriptions and PushTelemetry over the wire" do
    broker = Kafkaesque::MockBroker.new
    pushed_metrics = Bytes.empty
    pushed_subscription_id = -1

    broker.on_request(71_i16) do |decoder, version| # GetTelemetrySubscriptions
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)                                        # throttle_time_ms
      enc.write_int16(0_i16)                                    # error_code
      enc.io.write(Bytes.new(16, 7_u8))                         # client_instance_id
      enc.write_int32(55)                                       # subscription_id
      enc.write_compact_array([0_i8]) { |c| enc.write_int8(c) } # accepted_compression_types
      enc.write_int32(10000)                                    # push_interval_ms
      enc.write_int32(1048576)                                  # telemetry_max_bytes
      enc.write_boolean(false)                                  # delta_temporality
      enc.write_compact_array(["broker.request.count"]) { |m| enc.write_compact_string(m) }
      enc.write_tag_buffer
      io
    end

    broker.on_request(72_i16) do |decoder, version| # PushTelemetry
      client_instance_id = Bytes.new(16)
      decoder.io.read_fully(client_instance_id)
      pushed_subscription_id = decoder.read_int32
      decoder.read_boolean # terminating
      decoder.read_int8    # compression_type
      pushed_metrics = decoder.read_compact_bytes || Bytes.empty

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      enc.write_tag_buffer
      io
    end

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect

      sub = client.get_telemetry_subscription
      sub.error_code.should eq(0)
      sub.subscription_id.should eq(55)
      sub.requested_metrics.should eq(["broker.request.count"])

      push_resp = client.push_client_telemetry(sub.subscription_id, sub.client_instance_id, "payload-bytes".to_slice)
      push_resp.error_code.should eq(0)

      pushed_subscription_id.should eq(55)
      pushed_metrics.should eq("payload-bytes".to_slice)
    ensure
      broker.close
    end
  end
end

describe "KIP-932 Share Groups end-to-end (MockBroker)" do
  it "joins a share group, fetches, and piggybacks acknowledgements on the next fetch via Consumer#share_each" do
    broker = Kafkaesque::MockBroker.new
    topic_id = Bytes.new(16, 5_u8)
    fetch_calls = 0
    heartbeat_calls = 0
    observed_ack_batch = {-1_i64, -1_i64}

    broker.on_request(10_i16) do |decoder, version| # FindCoordinator (classic GROUP type, v2 — same as KIP-848)
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      enc.write_string(nil)  # error_message
      enc.write_int32(1)     # coordinator node_id
      enc.write_string("127.0.0.1")
      enc.write_int32(broker.port)
      io
    end

    broker.on_request(3_i16) do |decoder, version| # Metadata (v12)
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      write_mock_metadata_prefix(enc, 1, "127.0.0.1", broker.port)
      enc.write_compact_array(["share-topic"]) do |name|
        enc.write_int16(0_i16) # error_code
        enc.write_compact_string(name)
        enc.io.write(topic_id)
        enc.write_boolean(false) # is_internal
        enc.write_compact_array([0]) do |part|
          enc.write_int16(0_i16)
          enc.write_int32(part)
          enc.write_int32(1)
          enc.write_int32(-1) # leader_epoch
          enc.write_compact_array([1]) { |r| enc.write_int32(r) }
          enc.write_compact_array([1]) { |r| enc.write_int32(r) }
          enc.write_compact_array([] of Int32) { }
          enc.write_tag_buffer
        end
        enc.write_int32(-2147483648)
        enc.write_tag_buffer
      end
      enc.write_tag_buffer
      io
    end

    broker.on_request(76_i16) do |decoder, version| # ShareGroupHeartbeat (KIP-932)
      heartbeat_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)            # throttle_time_ms
      enc.write_int16(0_i16)        # error_code
      enc.write_compact_string(nil) # error_message
      enc.write_compact_string("share-member-e2e")
      enc.write_int32(1)     # member_epoch
      enc.write_int32(30000) # heartbeat_interval_ms
      enc.write_int8(1_i8)   # has_assignment: present
      enc.write_compact_array([topic_id]) do |uuid|
        enc.io.write(uuid)
        enc.write_compact_array([0]) { |p| enc.write_int32(p) }
        enc.write_tag_buffer
      end
      enc.write_tag_buffer # Assignment tag buffer
      enc.write_tag_buffer # Response tag buffer
      io
    end

    broker.on_request(78_i16) do |decoder, version| # ShareFetch (v1)
      fetch_calls += 1

      # Parse the request far enough to observe any piggybacked acknowledgement
      # batch for partition 0, proving the previous fetch's acquired records
      # were acked on this (the next) request rather than via a separate call.
      decoder.read_compact_string # group_id
      decoder.read_compact_string # member_id
      decoder.read_int32          # share_session_epoch
      decoder.read_int32          # max_wait_ms
      decoder.read_int32          # min_bytes
      decoder.read_int32          # max_bytes
      decoder.read_int32          # max_records
      decoder.read_int32          # batch_size
      decoder.read_compact_array do
        tid_buf = Bytes.new(16)
        decoder.io.read_fully(tid_buf)
        decoder.read_compact_array do
          decoder.read_int32 # partition_index
          batches = decoder.read_compact_array do
            first_offset = decoder.read_int64
            last_offset = decoder.read_int64
            decoder.read_compact_array { decoder.read_int8 }
            decoder.read_tag_buffer
            {first_offset, last_offset}
          end || [] of Tuple(Int64, Int64)
          observed_ack_batch = batches.first if batches.first?
          decoder.read_tag_buffer
        end
        decoder.read_tag_buffer
      end

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)            # throttle_time_ms
      enc.write_int16(0_i16)        # error_code
      enc.write_compact_string(nil) # error_message
      enc.write_int32(30000)        # acquisition_lock_timeout_ms
      enc.write_compact_array([topic_id]) do |tid|
        enc.io.write(tid)
        enc.write_compact_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16)        # partition error_code
          enc.write_compact_string(nil) # error_message
          enc.write_int16(0_i16)        # acknowledge_error_code
          enc.write_compact_string(nil) # acknowledge_error_message
          enc.write_int32(1)            # current_leader.leader_id
          enc.write_int32(0)            # current_leader.leader_epoch
          enc.write_tag_buffer          # current_leader tag buffer
          if fetch_calls <= 2
            offset = (fetch_calls - 1).to_i64
            record = Kafkaesque::Protocol::Record.new("k".to_slice, "share-value-#{fetch_calls}".to_slice, [] of Kafkaesque::Protocol::RecordHeader)
            record.offset = offset
            batch = Kafkaesque::Protocol::RecordBatch.new([record])
            batch_io = IO::Memory.new
            batch.serialize(batch_io)
            enc.write_compact_bytes(batch_io.to_slice)
            enc.write_compact_array([0]) do |_|
              enc.write_int64(offset) # acquired first_offset
              enc.write_int64(offset) # acquired last_offset
              enc.write_int16(1_i16)  # delivery_count
              enc.write_tag_buffer
            end
          else
            enc.write_compact_bytes(Bytes.empty)
            enc.write_compact_array([] of Int32) { }
          end
          enc.write_tag_buffer # partition tag buffer
        end
        enc.write_tag_buffer # topic tag buffer
      end
      enc.write_compact_array([] of Int32) { } # node_endpoints
      enc.write_tag_buffer
      io
    end

    begin
      config = Kafkaesque::Consumer::Config.new(
        ["127.0.0.1:#{broker.port}"],
        group_id: "share-group-e2e"
      )
      consumer = Kafkaesque::Consumer.new(config)
      consumer.subscribe(["share-topic"])

      received = [] of String
      spawn do
        consumer.share_each do |record|
          received << record.value.to_s
          consumer.close if received.size >= 2
        end
      end

      sleep 300.milliseconds

      received.should eq(["share-value-1", "share-value-2"])
      heartbeat_calls.should be >= 1
      fetch_calls.should be >= 2
      observed_ack_batch.should eq({0_i64, 0_i64})
    ensure
      broker.close
    end
  end
end

describe "KIP-392 Closest Replica Routing end-to-end (MockBroker)" do
  it "sends the configured client_rack as RackId on the wire in a v11+ Fetch request" do
    broker = Kafkaesque::MockBroker.new
    observed_rack = ""
    observed_version = 0_i16

    broker.on_request(3_i16) do |decoder, version|
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)                        # throttle_time_ms
      enc.write_compact_array([] of String) { } # brokers
      enc.write_compact_string(nil)             # cluster_id
      enc.write_int32(-1)                       # controller_id
      enc.write_compact_array([] of String) { } # topics
      enc.write_tag_buffer
      io
    end

    broker.on_request(1_i16) do |decoder, version|
      observed_version = version
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
          decoder.read_int32 # current_leader_epoch
          decoder.read_int64 # fetch_offset
          decoder.read_int64 # log_start_offset
          decoder.read_int32 # partition_max_bytes
        end
      end
      decoder.read_array { decoder.read_string } # forgotten_topics_data
      observed_rack = decoder.read_string.to_s

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)
      enc.write_int16(0_i16)
      enc.write_int32(0)
      enc.write_array([] of String) { }
      io
    end

    begin
      client = Kafkaesque::Client.new("127.0.0.1", broker.port)
      client.connect
      client.client_rack = "rack-c"
      client.fetch("rack-topic", 0, 0_i64)

      observed_version.should eq(11_i16)
      observed_rack.should eq("rack-c")
    ensure
      broker.close
    end
  end
end
