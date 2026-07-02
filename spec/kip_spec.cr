class MockConnection < Kafkaesque::Connection
  property closed : Bool = false

  def initialize
    @host = "127.0.0.1"
    @port = 9091
    @use_ssl = false
    @socket = Pointer(Void).null.as(TCPSocket)
  end

  def closed? : Bool
    @closed
  end

  def close
    @closed = true
  end
end

describe "Kafkaesque KIP-511 & KIP-392 Support" do
  describe "KIP-511 ApiVersions Serialization" do
    it "serializes ApiVersionsRequest with compact fields" do
      req = Kafkaesque::Protocol::ApiVersionsRequest.new("my-custom-client", "1.2.3")
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      req.serialize(enc)

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)

      # Read back COMPACT_STRINGS and verify they match
      dec.read_compact_string.should eq("my-custom-client")
      dec.read_compact_string.should eq("1.2.3")
    end

    it "deserializes ApiVersionsResponse correctly" do
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      enc.write_int16(0_i16) # error_code
      enc.write_compact_array([18_i16]) do |key|
        enc.write_int16(key)   # api_key
        enc.write_int16(0_i16) # min_version
        enc.write_int16(3_i16) # max_version
        enc.write_tag_buffer
      end
      enc.write_int32(100) # throttle_time_ms
      enc.write_tag_buffer # response tag buffer

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      resp = Kafkaesque::Protocol::ApiVersionsResponse.deserialize(dec)

      resp.error_code.should eq(0)
      resp.api_keys.size.should eq(1)
      resp.api_keys.first.api_key.should eq(18)
      resp.api_keys.first.min_version.should eq(0)
      resp.api_keys.first.max_version.should eq(3)
      resp.throttle_time_ms.should eq(100)
    end
  end

  describe "KIP-392 Closest Replica Routing" do
    it "routes fetches to a replica matching the client's rack" do
      client = Kafkaesque::Client.new("127.0.0.1", 9092)
      client.client_rack = "rack-az-2"

      # Setup fake metadata topology
      # Leader is broker 1 in rack-az-1
      # Follower is broker 2 in rack-az-2 (matching client's rack!)
      client.@partition_leaders["telemetry-topic:0"] = 1
      client.@partition_replicas["telemetry-topic:0"] = [1, 2]

      b1 = Kafkaesque::Protocol::Broker.new(1, "127.0.0.1", 9091, "rack-az-1")
      b2 = Kafkaesque::Protocol::Broker.new(2, "127.0.0.1", 9092, "rack-az-2")
      client.@brokers[1] = b1
      client.@brokers[2] = b2

      # Fake connection objects to bypass actual socket IO
      conn1 = MockConnection.new
      conn2 = MockConnection.new
      client.@broker_connections[1] = conn1
      client.@broker_connections[2] = conn2

      # Verify it routes to conn2 (broker 2 on rack-az-2)
      selected_conn = client.closest_replica_connection_for_partition("telemetry-topic", 0)
      selected_conn.should eq(conn2)
    end

    it "falls back to leader if no replicas match client's rack" do
      client = Kafkaesque::Client.new("127.0.0.1", 9092)
      client.client_rack = "rack-az-3" # No replica matches this rack

      client.@partition_leaders["telemetry-topic:0"] = 1
      client.@partition_replicas["telemetry-topic:0"] = [1, 2]

      b1 = Kafkaesque::Protocol::Broker.new(1, "127.0.0.1", 9091, "rack-az-1")
      b2 = Kafkaesque::Protocol::Broker.new(2, "127.0.0.1", 9092, "rack-az-2")
      client.@brokers[1] = b1
      client.@brokers[2] = b2

      conn1 = MockConnection.new
      conn2 = MockConnection.new
      client.@broker_connections[1] = conn1
      client.@broker_connections[2] = conn2

      # Verify it falls back to leader conn1
      selected_conn = client.closest_replica_connection_for_partition("telemetry-topic", 0)
      selected_conn.should eq(conn1)
    end
  end

  describe "ApiVersions integration with Client and MockBroker" do
    it "queries and stores API versions during client connection" do
      broker = Kafkaesque::MockBroker.new
      begin
        client = Kafkaesque::Client.new("127.0.0.1", broker.port)
        client.connect

        client.api_versions.should_not be_empty
        # Verify that ApiVersions API key 18 is retrieved
        client.api_versions.any? { |info| info.api_key == 18 }.should be_true
      ensure
        broker.close
      end
    end
  end

  describe "KIP-932 Share Groups API Serialization" do
    it "serializes ShareGroupHeartbeatRequest correctly" do
      req = Kafkaesque::Protocol::ShareGroupHeartbeatRequest.new(
        "share-group",
        "member-123",
        5,
        "rack-us-1",
        ["telemetry-topic"]
      )
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      req.serialize(enc)

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)

      dec.read_compact_string.should eq("share-group")
      dec.read_compact_string.should eq("member-123")
      dec.read_int32.should eq(5)
      dec.read_compact_string.should eq("rack-us-1")
      dec.read_compact_array { dec.read_compact_string }.should eq(["telemetry-topic"])
    end

    it "deserializes ShareGroupHeartbeatResponse correctly" do
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      enc.write_int32(0)            # throttle_time_ms
      enc.write_int16(0_i16)        # error_code
      enc.write_compact_string(nil) # error_message
      enc.write_compact_string("member-123")
      enc.write_int32(6)    # member_epoch
      enc.write_int32(5000) # heartbeat_interval
      enc.write_int8(-1_i8) # has_assignment: null
      enc.write_tag_buffer

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      resp = Kafkaesque::Protocol::ShareGroupHeartbeatResponse.deserialize(dec)

      resp.throttle_time_ms.should eq(0)
      resp.error_code.should eq(0)
      resp.error_message.should be_nil
      resp.member_id.should eq("member-123")
      resp.member_epoch.should eq(6)
      resp.heartbeat_interval_ms.should eq(5000)
      resp.assignment.should be_nil
    end

    it "deserializes ShareGroupHeartbeatResponse with a populated assignment" do
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # error_code
      enc.write_compact_string(nil)
      enc.write_compact_string("member-123")
      enc.write_int32(6)
      enc.write_int32(5000)
      enc.write_int8(1_i8) # has_assignment: present

      topic_id = Bytes.new(16, 3_u8)
      enc.write_compact_array([topic_id]) do |uuid|
        enc.io.write(uuid)
        enc.write_compact_array([0, 1]) { |p| enc.write_int32(p) }
        enc.write_tag_buffer
      end
      enc.write_tag_buffer # Assignment tag buffer
      enc.write_tag_buffer # Response tag buffer

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      resp = Kafkaesque::Protocol::ShareGroupHeartbeatResponse.deserialize(dec)

      assignment = resp.assignment.not_nil!
      assignment.topic_partitions.size.should eq(1)
      assignment.topic_partitions.first.topic_id.should eq(topic_id)
      assignment.topic_partitions.first.partitions.should eq([0, 1])
    end
  end

  describe "KIP-714 Client Telemetry API Serialization" do
    it "serializes GetTelemetrySubscriptionsRequest and deserializes GetTelemetrySubscriptionsResponse" do
      client_id = Bytes.new(16)
      client_id[0] = 5_u8

      req = Kafkaesque::Protocol::GetTelemetrySubscriptionsRequest.new(client_id)
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      req.serialize(enc)

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      read_id = Bytes.new(16)
      dec.io.read_fully(read_id)
      read_id.should eq(client_id)

      # Deserialization test
      io.clear
      enc.write_int32(0)                                        # throttle_time_ms
      enc.write_int16(0_i16)                                    # error_code
      enc.io.write(client_id)                                   # client_instance_id
      enc.write_int32(99)                                       # subscription_id
      enc.write_compact_array([1_i8]) { |c| enc.write_int8(c) } # accepted_compression_types
      enc.write_int32(30000)                                    # push_interval_ms
      enc.write_int32(2097152)                                  # telemetry_max_bytes
      enc.write_boolean(true)                                   # delta_temporality
      enc.write_compact_array(["metric-a"]) { |m| enc.write_compact_string(m) }
      enc.write_tag_buffer

      io.rewind
      resp = Kafkaesque::Protocol::GetTelemetrySubscriptionsResponse.deserialize(dec)
      resp.error_code.should eq(0)
      resp.client_instance_id.should eq(client_id)
      resp.subscription_id.should eq(99)
      resp.push_interval_ms.should eq(30000)
      resp.telemetry_max_bytes.should eq(2097152)
      resp.requested_metrics.should eq(["metric-a"])
    end
  end
end
