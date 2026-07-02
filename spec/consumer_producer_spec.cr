require "./spec_helper"
require "../src/kafkaesque/mock_broker"

describe Kafkaesque::Producer do
  it "correctly configures and parses high-level properties from settings" do
    config = Kafkaesque::Producer::Config.new(["localhost:9092"]).tap do |cfg|
      cfg.set("enable.idempotence", "true")
      cfg.set("acks", "all")
      cfg.set("linger.ms", "25")
      cfg.set("batch.num.messages", "5000")
      cfg.set("retries", "3")
      cfg.set("retry.backoff.ms", "250")
    end

    config.settings["enable.idempotence"].should eq("true")
    config.settings["acks"].should eq("all")
    config.settings["linger.ms"].should eq("25")
    config.settings["batch.num.messages"].should eq("5000")
    config.settings["retries"].should eq("3")
    config.settings["retry.backoff.ms"].should eq("250")
  end
end

describe Kafkaesque::Consumer do
  it "correctly configures and parses high-level properties from settings" do
    config = Kafkaesque::Consumer::Config.new(["localhost:9092"]).tap do |cfg|
      cfg.set("group.id", "mio-super-gruppo")
      cfg.set("group.instance.id", "pod-crystal-01")
      cfg.set("enable.auto.commit", "false")
      cfg.set("auto.offset.reset", "smallest")
      cfg.set("session.timeout.ms", "45000")
      cfg.set("heartbeat.interval.ms", "15000")
      cfg.set("fetch.min.bytes", "10240")
      cfg.set("queued.min.messages", "500000")
    end

    config.settings["group.id"].should eq("mio-super-gruppo")
    config.settings["group.instance.id"].should eq("pod-crystal-01")
    config.settings["enable.auto.commit"].should eq("false")
    config.settings["auto.offset.reset"].should eq("smallest")
    config.settings["session.timeout.ms"].should eq("45000")
    config.settings["heartbeat.interval.ms"].should eq("15000")
    config.settings["fetch.min.bytes"].should eq("10240")
    config.settings["queued.min.messages"].should eq("500000")
  end
end

describe Kafkaesque::Protocol::FetchRequest do
  it "serializes custom min_bytes value correctly" do
    req = Kafkaesque::Protocol::FetchRequest.new("test-topic", 0, 100_i64, min_bytes: 10240)
    req.min_bytes.should eq(10240)

    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)
    req.serialize(encoder)

    # Verify the serialized min_bytes field by parsing it back
    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)

    decoder.read_int32.should eq(-1)    # replica_id
    decoder.read_int32.should eq(1000)  # max_wait_ms
    decoder.read_int32.should eq(10240) # min_bytes (should match the custom configured value!)
  end
end

describe "Kafkaesque Record Headers & Serialization" do
  it "serializes and deserializes record headers correctly" do
    headers = [
      Kafkaesque::Protocol::RecordHeader.new("userid", "4636cba2-3f74-4098-8284-a68326e646ee"),
      Kafkaesque::Protocol::RecordHeader.new("correlationid", "uuid-random-abc"),
    ]
    record = Kafkaesque::Protocol::Record.new("sensor_key", "sensor_val", headers)

    io = IO::Memory.new
    record.serialize(io)

    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)

    # Skip record-batch metadata details to reach headers count
    size = decoder.read_varint
    decoder.read_int8    # attributes
    decoder.read_varlong # timestamp delta
    decoder.read_varint  # offset delta

    # Key
    k_len = decoder.read_varint
    k_len.should eq("sensor_key".size)
    io.skip(k_len)

    # Value
    v_len = decoder.read_varint
    v_len.should eq("sensor_val".size)
    io.skip(v_len)

    # Headers count
    h_count = decoder.read_varint
    h_count.should eq(2)

    # Verify first header
    h1_key_len = decoder.read_varint
    h1_key_bytes = Bytes.new(h1_key_len)
    io.read_fully(h1_key_bytes)
    String.new(h1_key_bytes).should eq("userid")

    h1_val_len = decoder.read_varint
    h1_val_bytes = Bytes.new(h1_val_len)
    io.read_fully(h1_val_bytes)
    String.new(h1_val_bytes).should eq("4636cba2-3f74-4098-8284-a68326e646ee")
  end
end

describe "Kafkaesque KIP-848 ConsumerGroupHeartbeat" do
  it "serializes and deserializes ConsumerGroupHeartbeatRequest successfully" do
    topic_id = Bytes.new(16, 7_u8)
    tp = Kafkaesque::Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions.new(topic_id, [0, 1, 2])

    req = Kafkaesque::Protocol::ConsumerGroupHeartbeatRequest.new(
      group_id: "test-group",
      member_id: "member-abc",
      member_epoch: 5,
      instance_id: "static-pod",
      subscribed_topic_names: ["topic-a"],
      topic_partitions: [tp]
    )

    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)
    req.serialize(encoder)

    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)

    # Verify serializations match properties
    decoder.read_compact_string.should eq("test-group")
    decoder.read_compact_string.should eq("member-abc")
    decoder.read_int32.should eq(5)
    decoder.read_compact_string.should eq("static-pod")
    decoder.read_compact_string.should be_nil # rack_id
    decoder.read_int32.should eq(30000)       # rebalance_timeout

    topics = decoder.read_compact_array { decoder.read_compact_string }
    topics.should eq(["topic-a"])

    decoder.read_compact_string.should be_nil # regex
    decoder.read_compact_string.should be_nil # server_assignor

    # Topic partitions array
    partitions_array = decoder.read_compact_array do
      r_uuid = Bytes.new(16)
      decoder.io.read_fully(r_uuid)
      r_uuid.should eq(topic_id)
      parts = decoder.read_compact_array { decoder.read_int32 }
      parts.should eq([0, 1, 2])
      decoder.read_tag_buffer
      true
    end
    partitions_array.not_nil!.size.should eq(1)
  end

  it "deserializes ConsumerGroupHeartbeatResponse with populated assignment" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    # throttle_time_ms, error_code, error_message, member_id, member_epoch, heartbeat_interval_ms
    encoder.write_int32(100)
    encoder.write_int16(0_i16)
    encoder.write_compact_string(nil)
    encoder.write_compact_string("assigned-member-id")
    encoder.write_int32(10)
    encoder.write_int32(5000)

    # has_assignment = 1 (Present)
    encoder.write_int8(1_i8)

    # Assignment: topic_partitions compact array (size 1)
    topic_id = Bytes.new(16, 9_u8)
    encoder.write_compact_array([topic_id]) do |uuid|
      encoder.io.write(uuid)
      encoder.write_compact_array([0, 1]) { |p| encoder.write_int32(p) }
      encoder.write_tag_buffer
    end
    encoder.write_tag_buffer # Assignment tag buffer
    encoder.write_tag_buffer # Response tag buffer

    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)
    resp = Kafkaesque::Protocol::ConsumerGroupHeartbeatResponse.deserialize(decoder)

    resp.error_code.should eq(0)
    resp.member_id.should eq("assigned-member-id")
    resp.member_epoch.should eq(10)
    resp.heartbeat_interval_ms.should eq(5000)

    assignment = resp.assignment.not_nil!
    assignment.topic_partitions.size.should eq(1)
    assignment.topic_partitions.first.topic_id.should eq(topic_id)
    assignment.topic_partitions.first.partitions.should eq([0, 1])
  end
end

describe "Kafkaesque SSL OIDC Configuration Settings" do
  it "allows creating clients with SASL token parameters mapped" do
    client = Kafkaesque::Client.new(
      host: "localhost",
      port: 9093,
      use_ssl: true,
      sasl_token: "mock-jwt-token"
    )
    client.client_id.should eq("kafkaesque-crystal")
  end
end

describe "Kafkaesque Compression Codecs" do
  it "compresses and decompresses records using gzip, snappy, lz4, and zstd correctly" do
    [1_i16, 2_i16, 3_i16, 4_i16].each do |codec|
      records = [
        Kafkaesque::Protocol::Record.new("key-a-#{codec}", "value-a-#{codec}"),
        Kafkaesque::Protocol::Record.new("key-b-#{codec}", "value-b-#{codec}"),
      ]
      batch = Kafkaesque::Protocol::RecordBatch.new(records, compression: codec)

      io = IO::Memory.new
      batch.serialize(io)

      io.rewind
      raw_bytes = io.to_slice

      deserialized = Kafkaesque::Protocol::RecordBatch.deserialize_from_bytes(raw_bytes)
      deserialized.size.should eq(2)
      deserialized[0].key.should eq("key-a-#{codec}")
      deserialized[0].value.should eq("value-a-#{codec}")
      deserialized[1].key.should eq("key-b-#{codec}")
      deserialized[1].value.should eq("value-b-#{codec}")
    end
  end
end

describe "Kafkaesque Dynamic OAUTHBEARER Token Resolution" do
  it "calls oauth_token_provider callback to obtain token" do
    called = false
    client = Kafkaesque::Client.new(
      host: "localhost",
      port: 9092,
      oauth_token_provider: -> {
        called = true
        "freshly-minted-token"
      }
    )
    client.oauth_token_provider.not_nil!.call.should eq("freshly-minted-token")
    called.should be_true
  end
end

describe "Kafkaesque Telemetry & Stats" do
  it "dispatches stats callback with json payloads" do
    client = Kafkaesque::Client.new(
      host: "localhost",
      port: 9092,
      client_id: "test-stats-client"
    )

    received_stats = ""
    client.on_stats do |stats|
      received_stats = stats
    end

    client.emit_stats
    received_stats.should_not be_empty
    received_stats.should contain("test-stats-client")
    received_stats.should contain("produced_messages")
    received_stats.should contain("broker_connections")
  end
end

describe "Kafkaesque Transactions Configuration" do
  it "correctly initializes producer in transactional mode with transactional.id" do
    config = Kafkaesque::Producer::Config.new(["localhost:9092"]).tap do |cfg|
      cfg.set("transactional.id", "my-tx-id")
    end
    config.settings["transactional.id"].should eq("my-tx-id")
  end

  it "registers the group via AddOffsetsToTxn before TxnOffsetCommit in #send_offsets_to_transaction" do
    broker = Kafkaesque::MockBroker.new
    add_offsets_calls = 0
    txn_offset_commit_calls = 0
    end_txn_calls = 0
    observed_group_id = ""

    broker.on_request(22_i16) do |decoder, version| # InitProducerId
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)      # throttle_time_ms
      enc.write_int16(0_i16)  # error_code
      enc.write_int64(42_i64) # producer_id
      enc.write_int16(0_i16)  # producer_epoch
      io
    end

    broker.on_request(25_i16) do |decoder, version| # AddOffsetsToTxn
      add_offsets_calls += 1
      decoder.read_string # transactional_id
      decoder.read_int64  # producer_id
      decoder.read_int16  # producer_epoch
      observed_group_id = decoder.read_string || ""

      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      io
    end

    broker.on_request(28_i16) do |decoder, version| # TxnOffsetCommit
      txn_offset_commit_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0) # throttle_time_ms
      enc.write_array(["my-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16) # error_code
        end
      end
      io
    end

    broker.on_request(26_i16) do |decoder, version| # EndTxn
      end_txn_calls += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      io
    end

    begin
      producer = Kafkaesque::Producer.new(
        Kafkaesque::Producer::Config.new(
          bootstrap_servers: ["127.0.0.1:#{broker.port}"],
          settings: {"transactional.id" => "tx-1"}
        )
      )

      producer.begin_transaction
      producer.send_offsets_to_transaction({"my-topic:0" => 5_i64}, "my-group")
      # A second call for the same group must not re-register via AddOffsetsToTxn.
      producer.send_offsets_to_transaction({"my-topic:0" => 6_i64}, "my-group")
      producer.commit_transaction

      observed_group_id.should eq("my-group")
      add_offsets_calls.should eq(1)
      txn_offset_commit_calls.should eq(2)
      end_txn_calls.should eq(1)
    ensure
      producer.try(&.close) rescue nil
      broker.close
    end
  end
end

describe "Kafkaesque Producer Compression Configurations" do
  it "correctly sets compression type in the Producer configuration" do
    ["gzip", "snappy", "lz4", "zstd"].each do |codec_name|
      config = Kafkaesque::Producer::Config.new(["localhost:9092"]).tap do |cfg|
        cfg.set("compression.type", codec_name)
      end
      config.settings["compression.type"].should eq(codec_name)
    end
  end

  it "correctly maps compression_type parameter on Config" do
    config = Kafkaesque::Producer::Config.new(["localhost:9092"], compression_type: "zstd")
    config.compression_type.should eq("zstd")
  end
end

describe "Kafkaesque Parity Features" do
  it "supports record timestamp field" do
    now = Time.utc
    rec = Kafkaesque::Protocol::Record.new("key", "val", timestamp: now)
    rec.timestamp.should eq(now)
  end

  it "supports subscribe splat syntax" do
    consumer = Kafkaesque::Consumer.new(Kafkaesque::Consumer::Config.new(["localhost:9092"]))
    consumer.subscribe("topic-a", "topic-b")
    # Verify subscribe splat compiled and ran
  end

  it "supports setting delivery and rebalance callbacks" do
    producer_config = Kafkaesque::Producer::Config.new(["localhost:9092"])
    # Don't try to initialize the real connection in tests without broker running,
    # but we can test Consumer callbacks setup
    consumer_config = Kafkaesque::Consumer::Config.new(["localhost:9092"])
    consumer = Kafkaesque::Consumer.new(consumer_config)

    assigned_called = false
    consumer.on_partitions_assigned do |parts|
      assigned_called = true
    end

    revoked_called = false
    consumer.on_partitions_revoked do |parts|
      revoked_called = true
    end

    # Trigger callbacks internally to verify they are stored and callable
    if cb = consumer.@on_partitions_assigned
      cb.call([0, 1])
    end
    assigned_called.should be_true

    if cb = consumer.@on_partitions_revoked
      cb.call([0, 1])
    end
    revoked_called.should be_true
  end
end
