module Kafkaesque
  module Protocol
    struct FindCoordinatorRequest
      API_KEY     = 10_i16
      API_VERSION =  2_i16

      property key : String
      property key_type : Int8

      def initialize(@key, @key_type = 0_i8)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@key)
        encoder.write_int8(@key_type)
      end
    end

    struct FindCoordinatorResponse
      property error_code : Int16
      property host : String
      property port : Int32

      def initialize(@error_code, @host, @port)
      end

      def self.deserialize(decoder : Decoder) : FindCoordinatorResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        decoder.read_string # error message
        decoder.read_int32  # coordinator node_id
        host = decoder.read_string.to_s
        port = decoder.read_int32
        FindCoordinatorResponse.new(error_code, host, port)
      end
    end

    struct GroupProtocol
      property name : String
      property metadata : Bytes

      def initialize(@name, @metadata = Bytes.empty)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@name)
        encoder.write_bytes(@metadata)
      end
    end

    struct JoinGroupRequest
      API_KEY     = 11_i16
      API_VERSION =  0_i16 # v0: session_timeout only, no rebalance_timeout; broker responds without throttle_time_ms

      property group_id : String
      property member_id : String
      property protocol_type : String
      property protocols : Array(GroupProtocol)

      def initialize(@group_id, @member_id, @protocol_type = "consumer", @protocols = [GroupProtocol.new("range")])
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(30000) # session_timeout_ms (v0 only has this)
        encoder.write_string(@member_id)
        encoder.write_string(@protocol_type)
        encoder.write_array(@protocols) do |proto|
          proto.serialize(encoder)
        end
      end
    end

    struct JoinGroupResponse
      property error_code : Int16
      property generation_id : Int32
      property protocol_name : String?
      property leader_id : String
      property member_id : String

      def initialize(@error_code, @generation_id, @protocol_name, @leader_id, @member_id)
      end

      def self.deserialize(decoder : Decoder) : JoinGroupResponse
        # v0 response: no throttle_time_ms field
        error_code = decoder.read_int16
        generation_id = decoder.read_int32
        protocol_name = decoder.read_string
        leader_id = decoder.read_string.to_s
        member_id = decoder.read_string.to_s

        # members array (only populated when we are the leader)
        decoder.read_array do
          decoder.read_string # member_id
          decoder.read_bytes  # metadata
        end

        JoinGroupResponse.new(error_code, generation_id, protocol_name, leader_id, member_id)
      end
    end

    struct SyncGroupRequest
      API_KEY     = 14_i16
      API_VERSION =  1_i16 # v1: no group_instance_id

      property group_id : String
      property generation_id : Int32
      property member_id : String

      def initialize(@group_id, @generation_id, @member_id)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@generation_id)
        encoder.write_string(@member_id)
        encoder.write_array([] of String) { } # assignments (empty for non-leader)
      end
    end

    struct SyncGroupResponse
      property error_code : Int16
      property assignment : Bytes?

      def initialize(@error_code, @assignment)
      end

      def self.deserialize(decoder : Decoder) : SyncGroupResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        assignment = decoder.read_bytes
        SyncGroupResponse.new(error_code, assignment)
      end
    end

    struct HeartbeatRequest
      API_KEY     = 12_i16
      API_VERSION =  1_i16 # v1: no group_instance_id

      property group_id : String
      property generation_id : Int32
      property member_id : String

      def initialize(@group_id, @generation_id, @member_id)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@generation_id)
        encoder.write_string(@member_id)
      end
    end

    struct HeartbeatResponse
      property error_code : Int16

      def initialize(@error_code)
      end

      def self.deserialize(decoder : Decoder) : HeartbeatResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        HeartbeatResponse.new(error_code)
      end
    end

    # ─── OffsetCommit (API key 8, version 4) ────────────────────────────────────
    # Persists the consumer offset for a topic+partition to the broker so the
    # group can resume at the right position after a restart or rebalance.

    struct OffsetCommitRequest
      API_KEY     = 8_i16
      API_VERSION = 2_i16 # v2: widely supported, includes retention_time_ms

      property group_id : String
      property generation_id : Int32
      property member_id : String
      property topic : String
      property partition : Int32
      property offset : Int64 # the *next* offset to be read (committed + 1)
      property metadata : String?

      def initialize(@group_id, @generation_id, @member_id,
                     @topic, @partition, @offset, @metadata = nil)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@group_id)
        encoder.write_int32(@generation_id)
        encoder.write_string(@member_id)
        encoder.write_int64(-1_i64) # retention_time_ms: -1 = use broker default
        encoder.write_array([@topic]) do |t|
          encoder.write_string(t)
          encoder.write_array([@partition]) do |p|
            encoder.write_int32(p)
            encoder.write_int64(@offset)
            encoder.write_int64(-1_i64) # timestamp (v2: -1 = wall clock)
            encoder.write_string(@metadata)
          end
        end
      end
    end

    struct OffsetCommitResponse
      property topic : String
      property partition : Int32
      property error_code : Int16

      def initialize(@topic, @partition, @error_code)
      end

      def self.deserialize(decoder : Decoder) : OffsetCommitResponse
        decoder.read_int32 # throttle_time_ms
        topic = ""
        partition = 0
        error_code = 0_i16
        decoder.read_array do
          topic = decoder.read_string.to_s
          decoder.read_array do
            partition = decoder.read_int32
            error_code = decoder.read_int16
          end
        end
        OffsetCommitResponse.new(topic, partition, error_code)
      end
    end

    # ─── OffsetFetch (API key 9, version 2) ─────────────────────────────────────
    # Retrieves the last committed offset for a topic+partition in a consumer group.
    # Returns -1 when no offset has been committed yet (start from earliest).

    struct OffsetFetchRequest
      API_KEY     = 9_i16
      API_VERSION = 1_i16 # v1: per-partition request, no top-level error_code in response

      property group_id : String
      property topic : String
      property partition : Int32

      def initialize(@group_id, @topic, @partition)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@group_id)
        encoder.write_array([@topic]) do |t|
          encoder.write_string(t)
          encoder.write_array([@partition]) do |p|
            encoder.write_int32(p)
          end
        end
      end
    end

    struct OffsetFetchResponse
      property topic : String
      property partition : Int32
      property committed_offset : Int64 # -1 means no committed offset yet
      property error_code : Int16

      def initialize(@topic, @partition, @committed_offset, @error_code)
      end

      def self.deserialize(decoder : Decoder) : OffsetFetchResponse
        decoder.read_int32 # throttle_time_ms
        topic = ""
        partition = 0
        committed_offset = -1_i64
        error_code = 0_i16
        decoder.read_array do
          topic = decoder.read_string.to_s
          decoder.read_array do
            partition = decoder.read_int32
            committed_offset = decoder.read_int64
            decoder.read_string # metadata (may be null)
            error_code = decoder.read_int16
          end
        end
        # v2 does NOT have a top-level error_code field
        OffsetFetchResponse.new(topic, partition, committed_offset, error_code)
      end
    end

    # ─── LeaveGroup (API key 13, version 1) ─────────────────────────────────────
    # Signals to the broker that this consumer is leaving the group gracefully,
    # triggering an immediate rebalance instead of waiting for session timeout.

    struct LeaveGroupRequest
      API_KEY     = 13_i16
      API_VERSION =  1_i16

      property group_id : String
      property member_id : String

      def initialize(@group_id, @member_id)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@group_id)
        encoder.write_string(@member_id)
      end
    end

    struct LeaveGroupResponse
      property error_code : Int16

      def initialize(@error_code)
      end

      def self.deserialize(decoder : Decoder) : LeaveGroupResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        LeaveGroupResponse.new(error_code)
      end
    end

    # ─── ConsumerGroupHeartbeat (API key 68, version 1) ──────────────────────────
    # Next-Generation consumer group protocol (KIP-848).

    struct ConsumerGroupHeartbeatRequest
      API_KEY     = 68_i16
      API_VERSION =  1_i16

      property group_id : String
      property member_id : String
      property member_epoch : Int32
      property instance_id : String?
      property rack_id : String?
      property rebalance_timeout_ms : Int32
      property subscribed_topic_names : Array(String)?
      property subscribed_topic_regex : String?
      property server_assignor : String?
      property topic_partitions : Array(TopicPartitions)

      struct TopicPartitions
        property topic_id : Bytes # 16 bytes uuid
        property partitions : Array(Int32)

        def initialize(@topic_id, @partitions)
        end

        def serialize(encoder : Encoder)
          encoder.io.write(@topic_id)
          encoder.write_compact_array(@partitions) do |p|
            encoder.write_int32(p)
          end
          encoder.write_tag_buffer
        end
      end

      def initialize(@group_id, @member_id, @member_epoch, @instance_id = nil, @rack_id = nil,
                     @rebalance_timeout_ms = 30000, @subscribed_topic_names = nil,
                     @subscribed_topic_regex = nil, @server_assignor = nil, @topic_partitions = [] of TopicPartitions)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_int32(@member_epoch)
        encoder.write_compact_string(@instance_id)
        encoder.write_compact_string(@rack_id)
        encoder.write_int32(@rebalance_timeout_ms)
        encoder.write_compact_array(@subscribed_topic_names) do |topic|
          encoder.write_compact_string(topic)
        end
        encoder.write_compact_string(@subscribed_topic_regex)
        encoder.write_compact_string(@server_assignor)
        encoder.write_compact_array(@topic_partitions) do |tp|
          tp.serialize(encoder)
        end
        encoder.write_tag_buffer
      end
    end

    struct ConsumerGroupHeartbeatResponse
      property throttle_time_ms : Int32
      property error_code : Int16
      property error_message : String?
      property member_id : String?
      property member_epoch : Int32
      property heartbeat_interval_ms : Int32
      property assignment : Assignment?

      struct Assignment
        property topic_partitions : Array(TopicPartitions)

        struct TopicPartitions
          property topic_id : Bytes
          property partitions : Array(Int32)

          def initialize(@topic_id, @partitions)
          end

          def self.deserialize(decoder : Decoder) : TopicPartitions
            topic_id = Bytes.new(16)
            decoder.io.read_fully(topic_id)
            partitions = decoder.read_compact_array { decoder.read_int32 } || [] of Int32
            decoder.read_tag_buffer
            TopicPartitions.new(topic_id, partitions)
          end
        end

        def initialize(@topic_partitions)
        end

        def self.deserialize(decoder : Decoder) : Assignment
          topic_partitions = decoder.read_compact_array { TopicPartitions.deserialize(decoder) } || [] of TopicPartitions
          decoder.read_tag_buffer
          Assignment.new(topic_partitions)
        end
      end

      def initialize(@throttle_time_ms, @error_code, @error_message, @member_id, @member_epoch, @heartbeat_interval_ms, @assignment)
      end

      def self.deserialize(decoder : Decoder) : ConsumerGroupHeartbeatResponse
        throttle_time_ms = decoder.read_int32
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        member_id = decoder.read_compact_string
        member_epoch = decoder.read_int32
        heartbeat_interval_ms = decoder.read_int32

        # Assignment structure (nullable, represented by compact struct deserialization)
        # Note: In Kafka's flexible schema, an optional struct can be represented as null.
        # Wait, the field itself might be a sub-struct. Let's decode it safely.
        assignment = nil
        has_assignment = decoder.read_int8
        if has_assignment > 0
          assignment = Assignment.deserialize(decoder)
        end

        decoder.read_tag_buffer
        ConsumerGroupHeartbeatResponse.new(
          throttle_time_ms, error_code, error_message, member_id, member_epoch, heartbeat_interval_ms, assignment
        )
      end
    end
  end
end
