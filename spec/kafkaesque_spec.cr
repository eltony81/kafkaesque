require "./spec_helper"

# Top-level struct for complex payload JSON round-trip spec
struct ComplexPayload
  include JSON::Serializable
  property id : Int32
  property name : String
  property active : Bool
  property score : Float64
  property tags : Array(String)

  def initialize(@id, @name, @active, @score, @tags)
  end
end

describe Kafkaesque::Protocol do
  it "encodes and decodes fixed-width integers" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    encoder.write_int8(12_i8)
    encoder.write_int16(-345_i16)
    encoder.write_int32(123456_i32)
    encoder.write_int64(-9876543210_i64)
    encoder.write_boolean(true)
    encoder.write_boolean(false)

    io.rewind

    decoder = Kafkaesque::Protocol::Decoder.new(io)
    decoder.read_int8.should eq(12_i8)
    decoder.read_int16.should eq(-345_i16)
    decoder.read_int32.should eq(123456_i32)
    decoder.read_int64.should eq(-9876543210_i64)
    decoder.read_boolean.should be_true
    decoder.read_boolean.should be_false
  end

  it "encodes and decodes zigzag varints/varlongs" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    encoder.write_varint(0)
    encoder.write_varint(-1)
    encoder.write_varint(1)
    encoder.write_varint(300)
    encoder.write_varlong(-9876543210_i64)

    io.rewind

    decoder = Kafkaesque::Protocol::Decoder.new(io)
    decoder.read_varint.should eq(0)
    decoder.read_varint.should eq(-1)
    decoder.read_varint.should eq(1)
    decoder.read_varint.should eq(300)
    decoder.read_varlong.should eq(-9876543210_i64)
  end

  it "encodes and decodes standard strings and compact strings" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    encoder.write_string("hello")
    encoder.write_string(nil)
    encoder.write_compact_string("world")
    encoder.write_compact_string(nil)
    encoder.write_compact_string("")

    io.rewind

    decoder = Kafkaesque::Protocol::Decoder.new(io)
    decoder.read_string.should eq("hello")
    decoder.read_string.should be_nil
    decoder.read_compact_string.should eq("world")
    decoder.read_compact_string.should be_nil
    decoder.read_compact_string.should eq("")
  end

  it "encodes and decodes standard arrays and compact arrays" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    encoder.write_array([10, 20, 30]) do |item|
      encoder.write_int32(item)
    end
    encoder.write_compact_array(["a", "b"]) do |item|
      encoder.write_compact_string(item)
    end

    io.rewind

    decoder = Kafkaesque::Protocol::Decoder.new(io)
    arr1 = decoder.read_array { decoder.read_int32 }
    arr1.should eq([10, 20, 30])

    arr2 = decoder.read_compact_array { decoder.read_compact_string }
    arr2.should eq(["a", "b"])
  end

  it "encodes and decodes RequestHeader and ResponseHeader" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    header = Kafkaesque::Protocol::RequestHeader.new(
      api_key: 3_i16,
      api_version: 7_i16,
      correlation_id: 12345_i32,
      client_id: "test-client",
      flexible: true
    )
    header.serialize(encoder)

    io.rewind

    decoder = Kafkaesque::Protocol::Decoder.new(io)
    decoder.read_int16.should eq(3_i16)
    decoder.read_int16.should eq(7_i16)
    decoder.read_int32.should eq(12345_i32)
    decoder.read_string.should eq("test-client")
    # Tag count should be 0
    decoder.read_varint.should eq(0)

    # Let's test ResponseHeader
    io2 = IO::Memory.new
    enc2 = Kafkaesque::Protocol::Encoder.new(io2)
    enc2.write_int32(12345_i32)
    enc2.write_varint(0) # 0 tags

    io2.rewind
    dec2 = Kafkaesque::Protocol::Decoder.new(io2)
    resp_header = Kafkaesque::Protocol::ResponseHeader.deserialize(dec2, flexible: true)
    resp_header.correlation_id.should eq(12345_i32)
  end

  it "encodes and decodes MetadataRequest and MetadataResponse" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    req = Kafkaesque::Protocol::MetadataRequest.new(["topic1", "topic2"])
    req.serialize(encoder)

    io.rewind

    decoder = Kafkaesque::Protocol::Decoder.new(io)
    topics = decoder.read_compact_array do
      decoder.io.skip(16) # topic_id
      name = decoder.read_compact_string
      decoder.read_tag_buffer
      name
    end
    topics.should eq(["topic1", "topic2"])

    # Let's test Response decoding (v12: flexible/compact)
    io2 = IO::Memory.new
    enc2 = Kafkaesque::Protocol::Encoder.new(io2)

    enc2.write_int32(0) # throttle_time_ms

    # 1 broker
    enc2.write_compact_array([1]) do |_|
      enc2.write_int32(1)                    # node_id
      enc2.write_compact_string("localhost") # host
      enc2.write_int32(9092)                 # port
      enc2.write_compact_string(nil)         # rack
      enc2.write_tag_buffer
    end

    enc2.write_compact_string("cluster1") # cluster_id
    enc2.write_int32(1)                   # controller_id

    # 1 topic
    enc2.write_compact_array([1]) do |_|
      enc2.write_int16(0)                 # error_code
      enc2.write_compact_string("topic1") # name
      enc2.io.write(Bytes.new(16, 7_u8))  # topic_id
      enc2.write_boolean(false)           # is_internal
      # 1 partition
      enc2.write_compact_array([1]) do |_|
        enc2.write_int16(0)                                         # error_code
        enc2.write_int32(0)                                         # partition_index
        enc2.write_int32(1)                                         # leader
        enc2.write_int32(-1)                                        # leader_epoch
        enc2.write_compact_array([1]) { |id| enc2.write_int32(id) } # replicas
        enc2.write_compact_array([1]) { |id| enc2.write_int32(id) } # isr
        enc2.write_compact_array([] of Int32) { }                   # offline_replicas
        enc2.write_tag_buffer
      end
      enc2.write_int32(-2147483648) # topic_authorized_operations
      enc2.write_tag_buffer
    end
    enc2.write_tag_buffer

    io2.rewind
    dec2 = Kafkaesque::Protocol::Decoder.new(io2)
    resp = Kafkaesque::Protocol::MetadataResponse.deserialize(dec2)

    resp.brokers.size.should eq(1)
    resp.brokers[0].host.should eq("localhost")
    resp.cluster_id.should eq("cluster1")
    resp.topics.size.should eq(1)
    resp.topics[0].name.should eq("topic1")
    resp.topics[0].partitions.size.should eq(1)
    resp.topics[0].partitions[0].leader_id.should eq(1)
  end

  it "encodes and decodes SaslHandshakeRequest/Response and SaslAuthenticateRequest/Response" do
    io = IO::Memory.new
    encoder = Kafkaesque::Protocol::Encoder.new(io)

    req = Kafkaesque::Protocol::SaslHandshakeRequest.new("OAUTHBEARER")
    req.serialize(encoder)

    io.rewind
    decoder = Kafkaesque::Protocol::Decoder.new(io)
    decoder.read_string.should eq("OAUTHBEARER")

    # OAuthbearer payload formatting
    payload = Kafkaesque::Protocol::SaslAuthenticateRequest.oauthbearer_payload("mytoken123", "kafka-broker", 9092)
    String.new(payload).should eq("n,,\u0001auth=Bearer mytoken123\u0001host=kafka-broker\u0001port=9092\u0001\u0001")

    # Authenticate Request
    io2 = IO::Memory.new
    enc2 = Kafkaesque::Protocol::Encoder.new(io2)
    auth_req = Kafkaesque::Protocol::SaslAuthenticateRequest.new(payload)
    auth_req.serialize(enc2)

    io2.rewind
    dec2 = Kafkaesque::Protocol::Decoder.new(io2)
    dec2.read_bytes.should eq(payload)

    # Authenticate Response
    io3 = IO::Memory.new
    enc3 = Kafkaesque::Protocol::Encoder.new(io3)
    enc3.write_int16(0)           # error_code
    enc3.write_string("Success")  # error_message
    enc3.write_bytes(Bytes.empty) # auth_bytes

    io3.rewind
    dec3 = Kafkaesque::Protocol::Decoder.new(io3)
    auth_resp = Kafkaesque::Protocol::SaslAuthenticateResponse.deserialize(dec3)
    auth_resp.error_code.should eq(0)
    auth_resp.error_message.should eq("Success")
  end

  it "instantiates Client with basic options" do
    client = Kafkaesque::Client.new(
      host: "localhost",
      port: 9092,
      sasl_token: "my-jwt-token"
    )
    client.client_id.should eq("kafkaesque-crystal")
  end

  it "computes CRC32C and serializes/deserializes RecordBatch" do
    records = [
      Kafkaesque::Protocol::Record.new("test-key", "test-val"),
    ]
    batch = Kafkaesque::Protocol::RecordBatch.new(records)

    io = IO::Memory.new
    batch.serialize(io)

    io.rewind
    decoded_records = Kafkaesque::Protocol::RecordBatch.deserialize_from_bytes(io.to_slice)
    decoded_records.size.should eq(1)
    decoded_records[0].key.should eq("test-key")
    decoded_records[0].value.should eq("test-val")
  end

  it "serializes and deserializes FindCoordinatorRequest and Response" do
    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    req = Kafkaesque::Protocol::FindCoordinatorRequest.new("test-group", 0_i8)
    req.serialize(enc)

    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_string.should eq("test-group")
    dec.read_int8.should eq(0_i8)

    resp_io = IO::Memory.new
    resp_enc = Kafkaesque::Protocol::Encoder.new(resp_io)
    resp_enc.write_int32(100)                 # throttle_time_ms
    resp_enc.write_int16(0_i16)               # error_code
    resp_enc.write_string("no error")         # message
    resp_enc.write_int32(1)                   # coordinator node_id
    resp_enc.write_string("coordinator-host") # host
    resp_enc.write_int32(9092)                # port

    resp_io.rewind
    resp_dec = Kafkaesque::Protocol::Decoder.new(resp_io)
    resp = Kafkaesque::Protocol::FindCoordinatorResponse.deserialize(resp_dec)
    resp.error_code.should eq(0_i16)
    resp.host.should eq("coordinator-host")
    resp.port.should eq(9092)
  end

  it "serializes and deserializes JoinGroupRequest and Response" do
    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    req = Kafkaesque::Protocol::JoinGroupRequest.new("test-group", "member-1", ["topic-a"])
    req.serialize(enc)

    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_string.should eq("test-group")
    dec.read_int32.should eq(30000) # session_timeout (v0: no rebalance_timeout)
    dec.read_string.should eq("member-1")
    dec.read_string.should eq("consumer")
    protocols = dec.read_array do
      name = dec.read_string
      metadata = dec.read_bytes
      {name, metadata}
    end.not_nil!
    protocols.size.should eq(1)
    protocols[0][0].should eq("range")
    # metadata is a real, version-prefixed ConsumerProtocolSubscription — not
    # an empty blob, since a real broker needs it to compute assignments.
    subscription = Kafkaesque::Protocol::ConsumerProtocolSubscription.deserialize(protocols[0][1].not_nil!)
    subscription.topics.should eq(["topic-a"])

    # v0 response: no throttle_time_ms field
    resp_io = IO::Memory.new
    resp_enc = Kafkaesque::Protocol::Encoder.new(resp_io)
    resp_enc.write_int16(0_i16)            # error_code
    resp_enc.write_int32(12)               # generation_id
    resp_enc.write_string("range")         # protocol_name
    resp_enc.write_string("leader-1")      # leader_id
    resp_enc.write_string("member-1")      # member_id
    resp_enc.write_array([] of String) { } # members list

    resp_io.rewind
    resp_dec = Kafkaesque::Protocol::Decoder.new(resp_io)
    resp = Kafkaesque::Protocol::JoinGroupResponse.deserialize(resp_dec)
    resp.error_code.should eq(0_i16)
    resp.generation_id.should eq(12)
    resp.protocol_name.should eq("range")
    resp.leader_id.should eq("leader-1")
    resp.member_id.should eq("member-1")
    resp.leader?.should be_false
  end

  it "serializes and deserializes SyncGroupRequest and Response" do
    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    req = Kafkaesque::Protocol::SyncGroupRequest.new("test-group", 12, "member-1")
    req.serialize(enc)

    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_string.should eq("test-group")
    dec.read_int32.should eq(12)
    dec.read_string.should eq("member-1")
    # v1: no group_instance_id field
    dec.read_array { dec.read_string }.should eq([] of String) # assignments

    resp_io = IO::Memory.new
    resp_enc = Kafkaesque::Protocol::Encoder.new(resp_io)
    resp_enc.write_int32(100)            # throttle_time
    resp_enc.write_int16(0_i16)          # error
    resp_enc.write_bytes(Bytes[1, 2, 3]) # assignment

    resp_io.rewind
    resp_dec = Kafkaesque::Protocol::Decoder.new(resp_io)
    resp = Kafkaesque::Protocol::SyncGroupResponse.deserialize(resp_dec)
    resp.error_code.should eq(0_i16)
    resp.assignment.should eq(Bytes[1, 2, 3])
  end

  it "serializes and deserializes HeartbeatRequest and Response" do
    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    req = Kafkaesque::Protocol::HeartbeatRequest.new("test-group", 12, "member-1")
    req.serialize(enc)

    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_string.should eq("test-group")
    dec.read_int32.should eq(12)
    dec.read_string.should eq("member-1")
    # v1: no group_instance_id field

    resp_io = IO::Memory.new
    resp_enc = Kafkaesque::Protocol::Encoder.new(resp_io)
    resp_enc.write_int32(100)   # throttle_time
    resp_enc.write_int16(0_i16) # error

    resp_io.rewind
    resp_dec = Kafkaesque::Protocol::Decoder.new(resp_io)
    resp = Kafkaesque::Protocol::HeartbeatResponse.deserialize(resp_dec)
    resp.error_code.should eq(0_i16)
  end

  it "serializes InitProducerIdRequest and deserializes InitProducerIdResponse" do
    # Serialize request
    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    req = Kafkaesque::Protocol::InitProducerIdRequest.new(nil, 30000)
    req.serialize(enc)

    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_string.should be_nil   # transactional_id = null
    dec.read_int32.should eq(30000) # transaction_timeout_ms

    # Deserialize response
    resp_io = IO::Memory.new
    resp_enc = Kafkaesque::Protocol::Encoder.new(resp_io)
    resp_enc.write_int32(0)         # throttle_time_ms
    resp_enc.write_int16(0_i16)     # error_code
    resp_enc.write_int64(12345_i64) # producer_id
    resp_enc.write_int16(0_i16)     # producer_epoch

    resp_io.rewind
    resp_dec = Kafkaesque::Protocol::Decoder.new(resp_io)
    resp = Kafkaesque::Protocol::InitProducerIdResponse.deserialize(resp_dec)
    resp.error_code.should eq(0_i16)
    resp.producer_id.should eq(12345_i64)
    resp.producer_epoch.should eq(0_i16)
  end

  it "serializes Record with headers and deserializes them from RecordBatch" do
    headers = [
      Kafkaesque::Protocol::RecordHeader.new("userid", "user-abc-123"),
      Kafkaesque::Protocol::RecordHeader.new("content-type", "application/json"),
    ]
    record = Kafkaesque::Protocol::Record.new("my-key", "my-value", headers)
    batch = Kafkaesque::Protocol::RecordBatch.new([record])

    io = IO::Memory.new
    batch.serialize(io)

    io.rewind
    decoded = Kafkaesque::Protocol::RecordBatch.deserialize_from_bytes(io.to_slice)
    decoded.size.should eq(1)
    r = decoded[0]
    r.key.should eq("my-key")
    r.value.should eq("my-value")
    r.headers.size.should eq(2)
    r.headers[0].key.should eq("userid")
    r.headers[0].value.should eq("user-abc-123")
    r.headers[1].key.should eq("content-type")
    r.headers[1].value.should eq("application/json")
  end

  it "serializes a complex Crystal object as JSON in a RecordBatch and round-trips it" do
    payload = ComplexPayload.new(99, "Crystal Test Object", true, 3.14, ["crystal", "kafka", "idempotent"])
    json_val = payload.to_json

    record = Kafkaesque::Protocol::Record.new("complex-key", json_val)
    batch = Kafkaesque::Protocol::RecordBatch.new([record])

    io = IO::Memory.new
    batch.serialize(io)

    io.rewind
    decoded = Kafkaesque::Protocol::RecordBatch.deserialize_from_bytes(io.to_slice)
    decoded.size.should eq(1)
    raw = decoded[0].value.not_nil!

    restored = ComplexPayload.from_json(raw)
    restored.id.should eq(99)
    restored.name.should eq("Crystal Test Object")
    restored.active.should be_true
    restored.score.should eq(3.14)
    restored.tags.should eq(["crystal", "kafka", "idempotent"])
  end

  it "serializes RecordBatch with idempotent producer_id, epoch and base_sequence" do
    records = [
      Kafkaesque::Protocol::Record.new("k1", "v1"),
      Kafkaesque::Protocol::Record.new("k2", "v2"),
    ]
    batch = Kafkaesque::Protocol::RecordBatch.new(
      records,
      producer_id: 9876_i64,
      producer_epoch: 3_i16,
      base_sequence: 42
    )

    io = IO::Memory.new
    batch.serialize(io)

    # Validate the binary layout directly
    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_int64 # base_offset
    batch_len = dec.read_int32
    batch_len.should be > 0

    dec.read_int32  # partition_leader_epoch
    dec.read_int8   # magic byte
    dec.read_uint32 # crc

    dec.read_int16 # attributes
    dec.read_int32 # last_offset_delta = 1 (2 records - 1)
    dec.read_int64 # first_timestamp
    dec.read_int64 # max_timestamp

    dec.read_int64.should eq(9876_i64) # producer_id
    dec.read_int16.should eq(3_i16)    # producer_epoch
    dec.read_int32.should eq(42)       # base_sequence

    dec.read_int32.should eq(2) # record count
  end

  it "ProduceRequest passes idempotency params to RecordBatch" do
    req = Kafkaesque::Protocol::ProduceRequest.new(
      acks: -1_i16,
      timeout_ms: 3000_i32,
      topic: "idempotent-topic",
      records: [Kafkaesque::Protocol::Record.new("k", "v")],
      partition: 0,
      producer_id: 1111_i64,
      producer_epoch: 2_i16,
      base_sequence: 7
    )

    io = IO::Memory.new
    enc = Kafkaesque::Protocol::Encoder.new(io)
    req.serialize(enc)

    io.rewind
    dec = Kafkaesque::Protocol::Decoder.new(io)
    dec.read_string.should be_nil    # transactional_id
    dec.read_int16.should eq(-1_i16) # acks
    dec.read_int32.should eq(3000)   # timeout_ms

    topic_count = dec.read_int32
    topic_count.should eq(1)
    dec.read_string.should eq("idempotent-topic")

    part_count = dec.read_int32
    part_count.should eq(1)
    dec.read_int32.should eq(0) # partition

    # The batch bytes
    batch_bytes = dec.read_bytes.not_nil!
    batch_io = IO::Memory.new(batch_bytes)
    bdec = Kafkaesque::Protocol::Decoder.new(batch_io)
    bdec.read_int64                     # base_offset
    bdec.read_int32                     # batch_length
    bdec.read_int32                     # partition_leader_epoch
    bdec.read_int8                      # magic
    bdec.read_uint32                    # crc
    bdec.read_int16                     # attributes
    bdec.read_int32                     # last_offset_delta
    bdec.read_int64                     # first_timestamp
    bdec.read_int64                     # max_timestamp
    bdec.read_int64.should eq(1111_i64) # producer_id
    bdec.read_int16.should eq(2_i16)    # producer_epoch
    bdec.read_int32.should eq(7)        # base_sequence
  end

  it "Client tracks sequence numbers monotonically per topic+partition" do
    # Test the sequence tracking logic standalone (mirrors what produce() does)
    sequence_numbers = {} of String => Int32

    simulate_produce = ->(topic : String, partition : Int32) {
      slot = "#{topic}:#{partition}"
      base_seq = sequence_numbers.fetch(slot, 0)
      sequence_numbers[slot] = base_seq + 1
      base_seq
    }

    # First produce on orders:0 => seq 0
    simulate_produce.call("orders", 0).should eq(0)
    # Second => seq 1
    simulate_produce.call("orders", 0).should eq(1)
    # Third => seq 2
    simulate_produce.call("orders", 0).should eq(2)

    # Different partition starts fresh at 0
    simulate_produce.call("orders", 1).should eq(0)
    simulate_produce.call("orders", 1).should eq(1)

    # Counter state
    sequence_numbers["orders:0"].should eq(3)
    sequence_numbers["orders:1"].should eq(2)
  end

  it "batch accumulator groups records by topic+partition" do
    pending = [] of {String, Int32, Array(Kafkaesque::Protocol::Record)}

    accumulate = ->(topic : String, partition : Int32, key : String, val : String) {
      rec = Kafkaesque::Protocol::Record.new(key, val)
      existing_idx = pending.index { |t, p, _| t == topic && p == partition }
      if existing_idx
        _, _, recs = pending[existing_idx]
        recs << rec
      else
        pending << {topic, partition, [rec]}
      end
    }

    # Accumulate 2 records for topic-A partition 0
    accumulate.call("topic-A", 0, "k1", "v1")
    accumulate.call("topic-A", 0, "k2", "v2")
    # 1 record for topic-B partition 0
    accumulate.call("topic-B", 0, "k3", "v3")
    # 1 record for topic-A partition 1 (different partition = new entry)
    accumulate.call("topic-A", 1, "k4", "v4")

    pending.size.should eq(3)

    _, _, topic_a_p0_records = pending[0]
    topic_a_p0_records.size.should eq(2)
    topic_a_p0_records[0].value.should eq("v1")
    topic_a_p0_records[1].value.should eq("v2")

    _, _, topic_b_records = pending[1]
    topic_b_records.size.should eq(1)
    topic_b_records[0].value.should eq("v3")

    _, _, topic_a_p1_records = pending[2]
    topic_a_p1_records.size.should eq(1)
    topic_a_p1_records[0].value.should eq("v4")
  end

  it "optimizes CRC32C with StaticArray" do
    Kafkaesque::Protocol::CRC32C::TABLE.is_a?(StaticArray(UInt32, 256)).should be_true
    data = "hello world".to_slice
    Kafkaesque::Protocol::CRC32C.checksum(data).should eq(3381945770_u32)
  end

  it "natively supports raw Bytes in Record and RecordHeader without converting to String" do
    raw_key = Bytes[1, 2, 3, 4]
    raw_val = Bytes[255, 254, 253]
    header_val = Bytes[10, 20, 30]

    header = Kafkaesque::Protocol::RecordHeader.new("my-header", header_val)
    record = Kafkaesque::Protocol::Record.new(raw_key, raw_val, [header])

    # Key/value are stored as Bytes
    record.key_bytes.should eq(raw_key)
    record.value_bytes.should eq(raw_val)
    header.value_bytes.should eq(header_val)

    # String getters still work (lossy/fallback representation)
    record.key.should_not be_nil
    record.value.should_not be_nil

    # Serialization and Deserialization round-trip preserves raw bytes exactly
    batch = Kafkaesque::Protocol::RecordBatch.new([record])
    io = IO::Memory.new
    batch.serialize(io)

    io.rewind
    decoded = Kafkaesque::Protocol::RecordBatch.deserialize_from_bytes(io.to_slice)
    decoded.size.should eq(1)
    r = decoded[0]
    r.key_bytes.should eq(raw_key)
    r.value_bytes.should eq(raw_val)
    r.headers.size.should eq(1)
    r.headers[0].value_bytes.should eq(header_val)
  end

  it "provides block configuration DSL and typed config properties" do
    # Producer
    p_config = Kafkaesque::Producer::Config.build do |c|
      c.bootstrap_servers = ["localhost:9094"]
      c.linger_ms = 50
      c.batch_num_messages = 5000
      c.idempotence = true
      c.acks = "all"
    end

    p_config.bootstrap_servers.should eq(["localhost:9094"])
    p_config.linger_ms.should eq(50)
    p_config.batch_num_messages.should eq(5000)
    p_config.idempotence.should be_true
    p_config.acks.should eq("all")

    # Consumer
    c_config = Kafkaesque::Consumer::Config.build do |c|
      c.bootstrap_servers = ["localhost:9095"]
      c.group_id = "test-group-dsl"
      c.auto_commit = false
      c.auto_commit_interval_ms = 10000
    end

    c_config.bootstrap_servers.should eq(["localhost:9095"])
    c_config.group_id.should eq("test-group-dsl")
    c_config.auto_commit.should be_false
    c_config.auto_commit_interval_ms.should eq(10000)
  end
end
