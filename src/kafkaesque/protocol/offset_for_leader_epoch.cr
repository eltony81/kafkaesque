module Kafkaesque
  module Protocol
    # OffsetForLeaderEpoch (KIP-320): used after a leader change to detect log
    # truncation — ask the new leader for the end offset of the epoch the
    # client was previously fetching under. If that end offset is lower than
    # the offset the client had already consumed up to, the old leader's log
    # was truncated (e.g. after an unclean leader election) and the client
    # must rewind to avoid silently skipping over data that no longer exists.
    #
    # Versions 0-1 were removed in Kafka 4.0; v2 is the current baseline and
    # is non-flexible (flexibleVersions starts at 4+).
    struct OffsetForLeaderEpochRequest
      API_KEY     = 23_i16
      API_VERSION =  2_i16

      def initialize(@topic : String, @partition : Int32, @current_leader_epoch : Int32, @leader_epoch : Int32)
      end

      def serialize(encoder : Encoder)
        encoder.write_array([@topic]) do |topic_name|
          encoder.write_string(topic_name)
          encoder.write_array([@partition]) do |part|
            encoder.write_int32(part)
            encoder.write_int32(@current_leader_epoch)
            encoder.write_int32(@leader_epoch)
          end
        end
      end
    end

    struct OffsetForLeaderEpochResponse
      property error_code : Int16
      property partition : Int32
      property leader_epoch : Int32
      property end_offset : Int64

      def initialize(@error_code, @partition, @leader_epoch, @end_offset)
      end

      def self.deserialize(decoder : Decoder) : OffsetForLeaderEpochResponse
        decoder.read_int32 # throttle_time_ms

        error_code = 0_i16
        partition = 0
        leader_epoch = -1
        end_offset = -1_i64

        decoder.read_array do
          decoder.read_string # topic
          decoder.read_array do
            error_code = decoder.read_int16
            partition = decoder.read_int32
            leader_epoch = decoder.read_int32
            end_offset = decoder.read_int64
          end
        end

        OffsetForLeaderEpochResponse.new(error_code, partition, leader_epoch, end_offset)
      end
    end
  end
end
