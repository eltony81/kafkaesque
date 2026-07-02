module Kafkaesque
  module Protocol
    # The "embedded" consumer protocol (not a top-level Kafka API) used inside
    # the opaque metadata/assignment byte blobs of JoinGroup/SyncGroup for the
    # classic ("range"/"roundrobin") consumer group protocol. Always
    # non-flexible, version-prefixed (an int16 version ahead of the fields
    # below) regardless of the outer request's own flexible-ness — see
    # ConsumerProtocolSubscription.json / ConsumerProtocolAssignment.json in
    # the upstream Kafka protocol schema. Version 0 (Topics + UserData /
    # AssignedPartitions + UserData) is all this client needs.
    struct ConsumerProtocolSubscription
      property topics : Array(String)

      def initialize(@topics)
      end

      def serialize(io : IO)
        encoder = Encoder.new(io)
        encoder.write_int16(0_i16) # version
        encoder.write_array(@topics) { |t| encoder.write_string(t) }
        encoder.write_bytes(nil) # user_data
      end

      def self.deserialize(bytes : Bytes) : ConsumerProtocolSubscription
        decoder = Decoder.new(IO::Memory.new(bytes))
        decoder.read_int16 # version
        topics = decoder.read_array { decoder.read_string.to_s } || [] of String
        ConsumerProtocolSubscription.new(topics)
      end
    end

    struct ConsumerProtocolAssignment
      property assigned_partitions : Hash(String, Array(Int32))

      def initialize(@assigned_partitions)
      end

      def serialize(io : IO)
        encoder = Encoder.new(io)
        encoder.write_int16(0_i16) # version
        encoder.write_array(@assigned_partitions.keys) do |topic|
          encoder.write_string(topic)
          encoder.write_array(@assigned_partitions[topic]) { |p| encoder.write_int32(p) }
        end
        encoder.write_bytes(nil) # user_data
      end

      def self.deserialize(bytes : Bytes) : ConsumerProtocolAssignment
        decoder = Decoder.new(IO::Memory.new(bytes))
        decoder.read_int16 # version
        assigned = {} of String => Array(Int32)
        decoder.read_array do
          topic = decoder.read_string.to_s
          assigned[topic] = decoder.read_array { decoder.read_int32 } || [] of Int32
        end
        ConsumerProtocolAssignment.new(assigned)
      end
    end

    # Classic Kafka's default "range" assignor: for each topic, sorts the
    # subscribed members lexicographically by member ID and hands out
    # contiguous partition ranges as evenly as possible (earlier members get
    # the extra partition when the count doesn't divide evenly) — matching
    # org.apache.kafka.clients.consumer.RangeAssignor.
    module RangeAssignor
      # members: member_id => subscribed topics. partitions_by_topic: topic => partition count.
      # Returns member_id => (topic => assigned partitions).
      def self.assign(members : Hash(String, Array(String)), partitions_by_topic : Hash(String, Int32)) : Hash(String, Hash(String, Array(Int32)))
        result = members.keys.to_h { |m| {m, {} of String => Array(Int32)} }

        topics = members.values.flatten.uniq
        topics.each do |topic|
          count = partitions_by_topic[topic]? || 0
          next if count == 0

          subscribed_members = members.select { |_, topics_for_member| topics_for_member.includes?(topic) }.keys.sort!
          next if subscribed_members.empty?

          partitions_per_member = count // subscribed_members.size
          extra = count % subscribed_members.size

          next_partition = 0
          subscribed_members.each_with_index do |member_id, idx|
            share = partitions_per_member + (idx < extra ? 1 : 0)
            assigned = (0...share).map { |i| next_partition + i }
            next_partition += share
            result[member_id][topic] = assigned unless assigned.empty?
          end
        end

        result
      end
    end
  end
end
