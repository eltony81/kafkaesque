module Kafkaesque
  module Protocol
    struct InitProducerIdRequest
      API_KEY     = 22_i16
      API_VERSION =  0_i16

      property transactional_id : String?
      property transaction_timeout_ms : Int32

      def initialize(@transactional_id = nil, @transaction_timeout_ms = 10000)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@transactional_id)
        encoder.write_int32(@transaction_timeout_ms)
      end
    end

    struct InitProducerIdResponse
      property error_code : Int16
      property producer_id : Int64
      property producer_epoch : Int16

      def initialize(@error_code, @producer_id, @producer_epoch)
      end

      def self.deserialize(decoder : Decoder) : InitProducerIdResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        producer_id = decoder.read_int64
        producer_epoch = decoder.read_int16
        InitProducerIdResponse.new(error_code, producer_id, producer_epoch)
      end
    end
  end
end
