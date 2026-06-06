module Kafkaesque
  module Protocol
    struct ShareGroupHeartbeatRequest
      API_KEY     = 85_i16
      API_VERSION =  0_i16

      property group_id : String
      property member_id : String
      property member_epoch : Int32
      property instance_id : String?
      property rack_id : String?
      property subscribed_topic_names : Array(String)

      def initialize(@group_id, @member_id, @member_epoch, @instance_id = nil, @rack_id = nil, @subscribed_topic_names = [] of String)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_int32(@member_epoch)
        encoder.write_compact_string(@instance_id)
        encoder.write_compact_string(@rack_id)
        encoder.write_compact_array(@subscribed_topic_names) do |topic|
          encoder.write_compact_string(topic)
        end
        encoder.write_tag_buffer
      end
    end

    struct ShareGroupHeartbeatResponse
      property error_code : Int16
      property error_message : String?
      property member_id : String?
      property member_epoch : Int32
      property heartbeat_interval_ms : Int32

      def initialize(@error_code, @error_message, @member_id, @member_epoch, @heartbeat_interval_ms)
      end

      def self.deserialize(decoder : Decoder) : ShareGroupHeartbeatResponse
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        member_id = decoder.read_compact_string
        member_epoch = decoder.read_int32
        heartbeat_interval_ms = decoder.read_int32
        decoder.read_tag_buffer
        ShareGroupHeartbeatResponse.new(error_code, error_message, member_id, member_epoch, heartbeat_interval_ms)
      end
    end

    # ShareFetch Request & Response (KIP-932)
    struct ShareFetchPartition
      property partition_index : Int32
      property max_bytes : Int32

      def initialize(@partition_index, @max_bytes = 1048576)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(@partition_index)
        encoder.write_int32(@max_bytes)
        encoder.write_tag_buffer
      end

      def self.deserialize(decoder : Decoder) : ShareFetchPartition
        part = decoder.read_int32
        max_b = decoder.read_int32
        decoder.read_tag_buffer
        ShareFetchPartition.new(part, max_b)
      end
    end

    struct ShareFetchTopic
      property name : String
      property partitions : Array(ShareFetchPartition)

      def initialize(@name, @partitions)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@name)
        encoder.write_compact_array(@partitions) do |part|
          part.serialize(encoder)
        end
        encoder.write_tag_buffer
      end

      def self.deserialize(decoder : Decoder) : ShareFetchTopic
        name = decoder.read_compact_string.to_s
        partitions = decoder.read_compact_array { ShareFetchPartition.deserialize(decoder) } || [] of ShareFetchPartition
        decoder.read_tag_buffer
        ShareFetchTopic.new(name, partitions)
      end
    end

    struct ShareFetchRequest
      API_KEY     = 78_i16
      API_VERSION =  0_i16

      property group_id : String
      property member_id : String
      property max_bytes : Int32
      property topics : Array(ShareFetchTopic)

      def initialize(@group_id, @member_id, @topics, @max_bytes = 1048576)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_int32(@max_bytes)
        encoder.write_compact_array(@topics) do |topic|
          topic.serialize(encoder)
        end
        encoder.write_tag_buffer
      end
    end

    struct ShareFetchResponsePartition
      property partition_index : Int32
      property error_code : Int16
      property records : Array(Record)

      def initialize(@partition_index, @error_code, @records)
      end

      def self.deserialize(decoder : Decoder) : ShareFetchResponsePartition
        partition_index = decoder.read_int32
        error_code = decoder.read_int16
        raw_bytes = decoder.read_compact_bytes
        records = [] of Record
        if raw_bytes && !raw_bytes.empty?
          records = RecordBatch.deserialize_from_bytes(raw_bytes, partition: partition_index)
        end
        decoder.read_tag_buffer
        ShareFetchResponsePartition.new(partition_index, error_code, records)
      end
    end

    struct ShareFetchResponseTopic
      property name : String
      property partitions : Array(ShareFetchResponsePartition)

      def initialize(@name, @partitions)
      end

      def self.deserialize(decoder : Decoder) : ShareFetchResponseTopic
        name = decoder.read_compact_string.to_s
        partitions = decoder.read_compact_array { ShareFetchResponsePartition.deserialize(decoder) } || [] of ShareFetchResponsePartition
        decoder.read_tag_buffer
        ShareFetchResponseTopic.new(name, partitions)
      end
    end

    struct ShareFetchResponse
      property error_code : Int16
      property error_message : String?
      property topics : Array(ShareFetchResponseTopic)

      def initialize(@error_code, @error_message, @topics)
      end

      def self.deserialize(decoder : Decoder) : ShareFetchResponse
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        topics = decoder.read_compact_array { ShareFetchResponseTopic.deserialize(decoder) } || [] of ShareFetchResponseTopic
        decoder.read_tag_buffer
        ShareFetchResponse.new(error_code, error_message, topics)
      end
    end

    # ShareAcknowledge Request & Response (KIP-932)
    struct ShareAckInfo
      property first_offset : Int64
      property last_offset : Int64
      property acknowledge_type : Int8 # 1 = ACK, 2 = REJECT, 3 = ABANDON

      def initialize(@first_offset, @last_offset, @acknowledge_type)
      end

      def serialize(encoder : Encoder)
        encoder.write_int64(@first_offset)
        encoder.write_int64(@last_offset)
        encoder.write_int8(@acknowledge_type)
        encoder.write_tag_buffer
      end

      def self.deserialize(decoder : Decoder) : ShareAckInfo
        f_off = decoder.read_int64
        l_off = decoder.read_int64
        ack_type = decoder.read_int8
        decoder.read_tag_buffer
        ShareAckInfo.new(f_off, l_off, ack_type)
      end
    end

    struct ShareAckPartition
      property partition_index : Int32
      property acknowledgements : Array(ShareAckInfo)

      def initialize(@partition_index, @acknowledgements)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(@partition_index)
        encoder.write_compact_array(@acknowledgements) do |ack|
          ack.serialize(encoder)
        end
        encoder.write_tag_buffer
      end

      def self.deserialize(decoder : Decoder) : ShareAckPartition
        part = decoder.read_int32
        acks = decoder.read_compact_array { ShareAckInfo.deserialize(decoder) } || [] of ShareAckInfo
        decoder.read_tag_buffer
        ShareAckPartition.new(part, acks)
      end
    end

    struct ShareAckTopic
      property name : String
      property partitions : Array(ShareAckPartition)

      def initialize(@name, @partitions)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@name)
        encoder.write_compact_array(@partitions) do |part|
          part.serialize(encoder)
        end
        encoder.write_tag_buffer
      end

      def self.deserialize(decoder : Decoder) : ShareAckTopic
        name = decoder.read_compact_string.to_s
        partitions = decoder.read_compact_array { ShareAckPartition.deserialize(decoder) } || [] of ShareAckPartition
        decoder.read_tag_buffer
        ShareAckTopic.new(name, partitions)
      end
    end

    struct ShareAcknowledgeRequest
      API_KEY     = 79_i16
      API_VERSION =  0_i16

      property group_id : String
      property member_id : String
      property topics : Array(ShareAckTopic)

      def initialize(@group_id, @member_id, @topics)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_compact_array(@topics) do |topic|
          topic.serialize(encoder)
        end
        encoder.write_tag_buffer
      end
    end

    struct ShareAcknowledgeResponse
      property error_code : Int16
      property error_message : String?

      def initialize(@error_code, @error_message)
      end

      def self.deserialize(decoder : Decoder) : ShareAcknowledgeResponse
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        decoder.read_tag_buffer
        ShareAcknowledgeResponse.new(error_code, error_message)
      end
    end
  end
end
