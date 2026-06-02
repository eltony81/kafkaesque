module Kafkaesque
  module Protocol
    struct AddPartitionsToTxnRequest
      API_KEY = 24_i16
      API_VERSION = 0_i16

      property transactional_id : String
      property producer_id : Int64
      property producer_epoch : Int16
      property topics : Hash(String, Array(Int32))

      def initialize(@transactional_id, @producer_id, @producer_epoch, @topics)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@transactional_id)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)

        encoder.write_array(@topics.keys) do |topic|
          encoder.write_string(topic)
          encoder.write_array(@topics[topic]) do |part|
            encoder.write_int32(part)
          end
        end
      end
    end

    struct AddPartitionsToTxnResponse
      property error_code : Int16 = 0_i16

      def initialize(@error_code)
      end

      def self.deserialize(decoder : Decoder) : AddPartitionsToTxnResponse
        decoder.read_int32 # throttle_time_ms
        # In v0, response has errors array per topic. Let's inspect if any error occurred.
        overall_error = 0_i16
        decoder.read_array do
          decoder.read_string # topic name
          decoder.read_array do
            decoder.read_int32 # partition
            err = decoder.read_int16
            if err != 0 && overall_error == 0
              overall_error = err
            end
          end
        end
        AddPartitionsToTxnResponse.new(overall_error)
      end
    end

    struct EndTxnRequest
      API_KEY = 26_i16
      API_VERSION = 0_i16

      property transactional_id : String
      property producer_id : Int64
      property producer_epoch : Int16
      property transaction_result : Bool # true = commit, false = abort

      def initialize(@transactional_id, @producer_id, @producer_epoch, @transaction_result)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@transactional_id)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)
        encoder.write_boolean(@transaction_result)
      end
    end

    struct EndTxnResponse
      property error_code : Int16

      def initialize(@error_code)
      end

      def self.deserialize(decoder : Decoder) : EndTxnResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        EndTxnResponse.new(error_code)
      end
    end

    struct TxnOffsetCommitRequest
      API_KEY = 28_i16
      API_VERSION = 0_i16

      property transactional_id : String
      property group_id : String
      property producer_id : Int64
      property producer_epoch : Int16
      property offsets : Hash(String, Hash(Int32, Int64)) # topic => partition => offset

      def initialize(@transactional_id, @group_id, @producer_id, @producer_epoch, @offsets)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@transactional_id)
        encoder.write_string(@group_id)
        encoder.write_int64(@producer_id)
        encoder.write_int16(@producer_epoch)

        encoder.write_array(@offsets.keys) do |topic|
          encoder.write_string(topic)
          encoder.write_array(@offsets[topic].keys) do |partition|
            offset = @offsets[topic][partition]
            encoder.write_int32(partition)
            encoder.write_int64(offset)
            encoder.write_string("") # metadata
          end
        end
      end
    end

    struct TxnOffsetCommitResponse
      property error_code : Int16

      def initialize(@error_code)
      end

      def self.deserialize(decoder : Decoder) : TxnOffsetCommitResponse
        decoder.read_int32 # throttle_time_ms
        overall_error = 0_i16
        decoder.read_array do
          decoder.read_string # topic name
          decoder.read_array do
            decoder.read_int32 # partition
            err = decoder.read_int16
            if err != 0 && overall_error == 0
              overall_error = err
            end
          end
        end
        TxnOffsetCommitResponse.new(overall_error)
      end
    end
  end
end
