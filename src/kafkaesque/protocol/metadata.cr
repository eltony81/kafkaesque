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
        host = decoder.read_string.to_s
        port = decoder.read_int32
        rack = decoder.read_string
        Broker.new(node_id, host, port, rack)
      end
    end

    struct PartitionMetadata
      property error_code : Int16
      property partition_index : Int32
      property leader_id : Int32
      property replica_nodes : Array(Int32)
      property isr_nodes : Array(Int32)

      def initialize(@error_code, @partition_index, @leader_id, @replica_nodes, @isr_nodes)
      end

      def self.deserialize(decoder : Decoder) : PartitionMetadata
        error_code = decoder.read_int16
        partition_index = decoder.read_int32
        leader_id = decoder.read_int32
        
        replica_nodes = decoder.read_array { decoder.read_int32 } || [] of Int32
        isr_nodes = decoder.read_array { decoder.read_int32 } || [] of Int32
        
        PartitionMetadata.new(error_code, partition_index, leader_id, replica_nodes, isr_nodes)
      end
    end

    struct TopicMetadata
      property error_code : Int16
      property name : String
      property is_internal : Bool
      property partitions : Array(PartitionMetadata)

      def initialize(@error_code, @name, @is_internal, @partitions)
      end

      def self.deserialize(decoder : Decoder) : TopicMetadata
        error_code = decoder.read_int16
        name = decoder.read_string.to_s
        is_internal = decoder.read_boolean
        partitions = decoder.read_array { PartitionMetadata.deserialize(decoder) } || [] of PartitionMetadata
        TopicMetadata.new(error_code, name, is_internal, partitions)
      end
    end

    struct MetadataRequest
      API_KEY = 3_i16
      API_VERSION = 2_i16

      property topics : Array(String)?

      def initialize(@topics = nil)
      end

      def serialize(encoder : Encoder)
        encoder.write_array(@topics) do |topic|
          encoder.write_string(topic)
        end
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
        brokers = decoder.read_array { Broker.deserialize(decoder) } || [] of Broker
        cluster_id = decoder.read_string
        controller_id = decoder.read_int32
        topics = decoder.read_array { TopicMetadata.deserialize(decoder) } || [] of TopicMetadata
        MetadataResponse.new(brokers, cluster_id, controller_id, topics)
      end
    end
  end
end
