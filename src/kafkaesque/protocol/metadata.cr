module Kafkaesque
  module Protocol
    struct Broker
      property node_id : Int32
      property host : String
      property port : Int32
      property rack : String?

      def initialize(@node_id, @host, @port, @rack)
      end

      def self.deserialize(decoder : Decoder) : Broker
        node_id = decoder.read_int32
        host = decoder.read_compact_string.to_s
        port = decoder.read_int32
        rack = decoder.read_compact_string
        decoder.read_tag_buffer
        Broker.new(node_id, host, port, rack)
      end
    end

    struct PartitionMetadata
      property error_code : Int16
      property partition_index : Int32
      property leader_id : Int32
      property leader_epoch : Int32
      property replica_nodes : Array(Int32)
      property isr_nodes : Array(Int32)

      def initialize(@error_code, @partition_index, @leader_id, @replica_nodes, @isr_nodes, @leader_epoch = -1)
      end

      def self.deserialize(decoder : Decoder) : PartitionMetadata
        error_code = decoder.read_int16
        partition_index = decoder.read_int32
        leader_id = decoder.read_int32
        leader_epoch = decoder.read_int32 # (7+)

        replica_nodes = decoder.read_compact_array { decoder.read_int32 } || [] of Int32
        isr_nodes = decoder.read_compact_array { decoder.read_int32 } || [] of Int32
        decoder.read_compact_array { decoder.read_int32 } # offline_replicas (5+)
        decoder.read_tag_buffer

        PartitionMetadata.new(error_code, partition_index, leader_id, replica_nodes, isr_nodes, leader_epoch)
      end
    end

    struct TopicMetadata
      property error_code : Int16
      property name : String
      property topic_id : Bytes # 16 bytes uuid (10+)
      property is_internal : Bool
      property partitions : Array(PartitionMetadata)

      def initialize(@error_code, @name, @topic_id, @is_internal, @partitions)
      end

      def self.deserialize(decoder : Decoder) : TopicMetadata
        error_code = decoder.read_int16
        name = decoder.read_compact_string.to_s
        topic_id = Bytes.new(16)
        decoder.io.read_fully(topic_id)
        is_internal = decoder.read_boolean
        partitions = decoder.read_compact_array { PartitionMetadata.deserialize(decoder) } || [] of PartitionMetadata
        decoder.read_int32 # topic_authorized_operations (8+)
        decoder.read_tag_buffer
        TopicMetadata.new(error_code, name, topic_id, is_internal, partitions)
      end
    end

    struct MetadataRequest
      API_KEY     =  3_i16
      API_VERSION = 12_i16 # v12: flexible (9+), adds TopicId to the response (needed for KIP-932 coordinator keys)

      property topics : Array(String)?

      def initialize(@topics = nil)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_array(@topics) do |topic|
          encoder.io.write(Bytes.new(16)) # topic_id (10+): unset, resolve by name
          encoder.write_compact_string(topic)
          encoder.write_tag_buffer
        end
        encoder.write_boolean(true)  # allow_auto_topic_creation (4+)
        encoder.write_boolean(false) # include_topic_authorized_operations (8+)
        encoder.write_tag_buffer
      end
    end

    struct MetadataResponse
      property brokers : Array(Broker)
      property cluster_id : String?
      property controller_id : Int32
      property topics : Array(TopicMetadata)

      def initialize(@brokers, @cluster_id, @controller_id, @topics)
      end

      def self.deserialize(decoder : Decoder) : MetadataResponse
        decoder.read_int32 # throttle_time_ms (3+)
        brokers = decoder.read_compact_array { Broker.deserialize(decoder) } || [] of Broker
        cluster_id = decoder.read_compact_string
        controller_id = decoder.read_int32
        topics = decoder.read_compact_array { TopicMetadata.deserialize(decoder) } || [] of TopicMetadata
        decoder.read_tag_buffer
        MetadataResponse.new(brokers, cluster_id, controller_id, topics)
      end
    end
  end
end
