module Kafkaesque
  module Protocol
    struct ShareGroupHeartbeatRequest
      API_KEY     = 76_i16
      API_VERSION =  1_i16 # v1: stable KIP-932 (v0 was early-access-only, removed in Kafka 4.1). Fields unchanged from v0.

      property group_id : String
      property member_id : String
      property member_epoch : Int32
      property rack_id : String?
      property subscribed_topic_names : Array(String)

      def initialize(@group_id, @member_id, @member_epoch, @rack_id = nil, @subscribed_topic_names = [] of String)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_int32(@member_epoch)
        encoder.write_compact_string(@rack_id)
        encoder.write_compact_array(@subscribed_topic_names) do |topic|
          encoder.write_compact_string(topic)
        end
        encoder.write_tag_buffer
      end
    end

    struct ShareGroupHeartbeatResponse
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

      def initialize(@throttle_time_ms, @error_code, @error_message, @member_id, @member_epoch, @heartbeat_interval_ms, @assignment = nil)
      end

      def self.deserialize(decoder : Decoder) : ShareGroupHeartbeatResponse
        throttle_time_ms = decoder.read_int32
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        member_id = decoder.read_compact_string
        member_epoch = decoder.read_int32
        heartbeat_interval_ms = decoder.read_int32

        assignment = nil
        has_assignment = decoder.read_int8
        if has_assignment > 0
          assignment = Assignment.deserialize(decoder)
        end

        decoder.read_tag_buffer
        ShareGroupHeartbeatResponse.new(throttle_time_ms, error_code, error_message, member_id, member_epoch, heartbeat_interval_ms, assignment)
      end
    end

    # ─── ShareFetch / ShareAcknowledge (KIP-932, API v1 — Kafka 4.1.0+) ────────
    # v1 replaces v0's name-addressed, ack-is-a-separate-call design with:
    #   - topic-ID addressing (like Fetch v13+ / KIP-516)
    #   - a stateful "share session" (ShareSessionEpoch, like incremental Fetch
    #     sessions from KIP-227): 0 opens a session, it increments by 1 each
    #     subsequent request, -1 closes it.
    #   - acknowledgements of previously-fetched records piggybacked directly
    #     on the next ShareFetchRequest (AcknowledgementBatches per partition),
    #     rather than requiring a separate ShareAcknowledgeRequest round trip.
    #     ShareAcknowledgeRequest still exists for ack-only calls (e.g. a final
    #     flush before closing a session without fetching more data).

    struct ShareAcknowledgementBatch
      property first_offset : Int64
      property last_offset : Int64
      # 0=Gap, 1=Accept, 2=Release, 3=Reject. A single-element array applies
      # that type to every record in [first_offset, last_offset].
      property acknowledge_types : Array(Int8)

      def initialize(@first_offset, @last_offset, @acknowledge_types)
      end

      def serialize(encoder : Encoder)
        encoder.write_int64(@first_offset)
        encoder.write_int64(@last_offset)
        encoder.write_compact_array(@acknowledge_types) { |t| encoder.write_int8(t) }
        encoder.write_tag_buffer
      end
    end

    struct ShareFetchPartitionRequest
      property partition_index : Int32
      property acknowledgement_batches : Array(ShareAcknowledgementBatch)

      def initialize(@partition_index, @acknowledgement_batches = [] of ShareAcknowledgementBatch)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(@partition_index)
        encoder.write_compact_array(@acknowledgement_batches) { |b| b.serialize(encoder) }
        encoder.write_tag_buffer
      end
    end

    struct ShareFetchTopicRequest
      property topic_id : Bytes
      property partitions : Array(ShareFetchPartitionRequest)

      def initialize(@topic_id, @partitions)
      end

      def serialize(encoder : Encoder)
        encoder.io.write(@topic_id)
        encoder.write_compact_array(@partitions) { |p| p.serialize(encoder) }
        encoder.write_tag_buffer
      end
    end

    struct ShareFetchRequest
      API_KEY     = 78_i16
      API_VERSION =  1_i16

      property group_id : String?
      property member_id : String?
      property share_session_epoch : Int32
      property topics : Array(ShareFetchTopicRequest)
      property max_wait_ms : Int32
      property min_bytes : Int32
      property max_bytes : Int32
      property max_records : Int32
      property batch_size : Int32

      def initialize(@group_id, @member_id, @share_session_epoch, @topics,
                     @max_wait_ms = 1000, @min_bytes = 1, @max_bytes = 1048576,
                     @max_records = 500, @batch_size = 500)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_int32(@share_session_epoch)
        encoder.write_int32(@max_wait_ms)
        encoder.write_int32(@min_bytes)
        encoder.write_int32(@max_bytes)
        encoder.write_int32(@max_records)
        encoder.write_int32(@batch_size)
        encoder.write_compact_array(@topics) { |t| t.serialize(encoder) }
        encoder.write_compact_array([] of Bytes) { } # forgotten_topics_data (no incremental removal)
        encoder.write_tag_buffer
      end
    end

    struct ShareAcquiredRecord
      property first_offset : Int64
      property last_offset : Int64
      property delivery_count : Int16

      def initialize(@first_offset, @last_offset, @delivery_count)
      end

      def self.deserialize(decoder : Decoder) : ShareAcquiredRecord
        first_offset = decoder.read_int64
        last_offset = decoder.read_int64
        delivery_count = decoder.read_int16
        decoder.read_tag_buffer
        ShareAcquiredRecord.new(first_offset, last_offset, delivery_count)
      end
    end

    struct ShareFetchResponsePartition
      property partition_index : Int32
      property error_code : Int16
      property acknowledge_error_code : Int16
      property records : Array(Record)
      property acquired_records : Array(ShareAcquiredRecord)

      def initialize(@partition_index, @error_code, @acknowledge_error_code, @records, @acquired_records)
      end

      def self.deserialize(decoder : Decoder) : ShareFetchResponsePartition
        partition_index = decoder.read_int32
        error_code = decoder.read_int16
        decoder.read_compact_string # error_message
        acknowledge_error_code = decoder.read_int16
        decoder.read_compact_string # acknowledge_error_message
        decoder.read_int32          # current_leader.leader_id
        decoder.read_int32          # current_leader.leader_epoch
        decoder.read_tag_buffer     # current_leader tag buffer

        raw_bytes = decoder.read_compact_bytes
        records = [] of Record
        if raw_bytes && !raw_bytes.empty?
          records = RecordBatch.deserialize_from_bytes(raw_bytes, partition: partition_index)
        end

        acquired_records = decoder.read_compact_array { ShareAcquiredRecord.deserialize(decoder) } || [] of ShareAcquiredRecord
        decoder.read_tag_buffer

        ShareFetchResponsePartition.new(partition_index, error_code, acknowledge_error_code, records, acquired_records)
      end
    end

    struct ShareFetchResponseTopic
      property topic_id : Bytes
      property partitions : Array(ShareFetchResponsePartition)

      def initialize(@topic_id, @partitions)
      end

      def self.deserialize(decoder : Decoder) : ShareFetchResponseTopic
        topic_id = Bytes.new(16)
        decoder.io.read_fully(topic_id)
        partitions = decoder.read_compact_array { ShareFetchResponsePartition.deserialize(decoder) } || [] of ShareFetchResponsePartition
        decoder.read_tag_buffer
        ShareFetchResponseTopic.new(topic_id, partitions)
      end
    end

    def self.skip_node_endpoints(decoder : Decoder)
      decoder.read_compact_array do
        decoder.read_int32          # node_id
        decoder.read_compact_string # host
        decoder.read_int32          # port
        decoder.read_compact_string # rack
        decoder.read_tag_buffer
      end
    end

    struct ShareFetchResponse
      property error_code : Int16
      property error_message : String?
      property acquisition_lock_timeout_ms : Int32
      property topics : Array(ShareFetchResponseTopic)

      def initialize(@error_code, @error_message, @acquisition_lock_timeout_ms, @topics)
      end

      def self.deserialize(decoder : Decoder) : ShareFetchResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        acquisition_lock_timeout_ms = decoder.read_int32
        topics = decoder.read_compact_array { ShareFetchResponseTopic.deserialize(decoder) } || [] of ShareFetchResponseTopic
        Protocol.skip_node_endpoints(decoder)
        decoder.read_tag_buffer
        ShareFetchResponse.new(error_code, error_message, acquisition_lock_timeout_ms, topics)
      end
    end

    struct ShareAcknowledgePartitionRequest
      property partition_index : Int32
      property acknowledgement_batches : Array(ShareAcknowledgementBatch)

      def initialize(@partition_index, @acknowledgement_batches)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(@partition_index)
        encoder.write_compact_array(@acknowledgement_batches) { |b| b.serialize(encoder) }
        encoder.write_tag_buffer
      end
    end

    struct ShareAcknowledgeTopicRequest
      property topic_id : Bytes
      property partitions : Array(ShareAcknowledgePartitionRequest)

      def initialize(@topic_id, @partitions)
      end

      def serialize(encoder : Encoder)
        encoder.io.write(@topic_id)
        encoder.write_compact_array(@partitions) { |p| p.serialize(encoder) }
        encoder.write_tag_buffer
      end
    end

    struct ShareAcknowledgeRequest
      API_KEY     = 79_i16
      API_VERSION =  1_i16

      property group_id : String?
      property member_id : String?
      property share_session_epoch : Int32
      property topics : Array(ShareAcknowledgeTopicRequest)

      def initialize(@group_id, @member_id, @share_session_epoch, @topics)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@group_id)
        encoder.write_compact_string(@member_id)
        encoder.write_int32(@share_session_epoch)
        encoder.write_compact_array(@topics) { |t| t.serialize(encoder) }
        encoder.write_tag_buffer
      end
    end

    struct ShareAcknowledgeResponsePartition
      property partition_index : Int32
      property error_code : Int16

      def initialize(@partition_index, @error_code)
      end

      def self.deserialize(decoder : Decoder) : ShareAcknowledgeResponsePartition
        partition_index = decoder.read_int32
        error_code = decoder.read_int16
        decoder.read_compact_string # error_message
        decoder.read_int32          # current_leader.leader_id
        decoder.read_int32          # current_leader.leader_epoch
        decoder.read_tag_buffer     # current_leader tag buffer
        decoder.read_tag_buffer     # partition tag buffer
        ShareAcknowledgeResponsePartition.new(partition_index, error_code)
      end
    end

    struct ShareAcknowledgeResponseTopic
      property topic_id : Bytes
      property partitions : Array(ShareAcknowledgeResponsePartition)

      def initialize(@topic_id, @partitions)
      end

      def self.deserialize(decoder : Decoder) : ShareAcknowledgeResponseTopic
        topic_id = Bytes.new(16)
        decoder.io.read_fully(topic_id)
        partitions = decoder.read_compact_array { ShareAcknowledgeResponsePartition.deserialize(decoder) } || [] of ShareAcknowledgeResponsePartition
        decoder.read_tag_buffer
        ShareAcknowledgeResponseTopic.new(topic_id, partitions)
      end
    end

    struct ShareAcknowledgeResponse
      property error_code : Int16
      property error_message : String?
      property topics : Array(ShareAcknowledgeResponseTopic)

      def initialize(@error_code, @error_message, @topics)
      end

      def self.deserialize(decoder : Decoder) : ShareAcknowledgeResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        error_message = decoder.read_compact_string
        topics = decoder.read_compact_array { ShareAcknowledgeResponseTopic.deserialize(decoder) } || [] of ShareAcknowledgeResponseTopic
        Protocol.skip_node_endpoints(decoder)
        decoder.read_tag_buffer
        ShareAcknowledgeResponse.new(error_code, error_message, topics)
      end
    end
  end
end
