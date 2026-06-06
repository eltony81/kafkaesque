module Kafkaesque
  module Protocol
    struct GetTelemetrySubscriptionsRequest
      API_KEY     = 71_i16
      API_VERSION =  0_i16

      property client_instance_id : Bytes # 16 bytes

      def initialize(@client_instance_id = Bytes.new(16))
      end

      def serialize(encoder : Encoder)
        encoder.io.write(@client_instance_id)
        encoder.write_tag_buffer
      end
    end

    struct GetTelemetrySubscriptionsResponse
      property throttle_time_ms : Int32
      property error_code : Int16
      property client_instance_id : Bytes # 16 bytes
      property subscription_id : Int32
      property accepted_compression_types : Array(Int8)
      property push_interval_ms : Int32
      property delta_temporality : Bool
      property requested_metrics : Array(String)

      def initialize(
        @throttle_time_ms,
        @error_code,
        @client_instance_id,
        @subscription_id,
        @accepted_compression_types,
        @push_interval_ms,
        @delta_temporality,
        @requested_metrics,
      )
      end

      def self.deserialize(decoder : Decoder) : GetTelemetrySubscriptionsResponse
        throttle_time_ms = decoder.read_int32
        error_code = decoder.read_int16

        client_instance_id = Bytes.new(16)
        decoder.io.read_fully(client_instance_id)

        subscription_id = decoder.read_int32
        accepted_compression_types = decoder.read_compact_array { decoder.read_int8 } || [] of Int8
        push_interval_ms = decoder.read_int32
        delta_temporality = decoder.read_boolean
        requested_metrics = decoder.read_compact_array { decoder.read_compact_string.to_s } || [] of String

        decoder.read_tag_buffer

        GetTelemetrySubscriptionsResponse.new(
          throttle_time_ms,
          error_code,
          client_instance_id,
          subscription_id,
          accepted_compression_types,
          push_interval_ms,
          delta_temporality,
          requested_metrics
        )
      end
    end

    struct PushTelemetryRequest
      API_KEY     = 72_i16
      API_VERSION =  0_i16

      property client_instance_id : Bytes # 16 bytes
      property subscription_id : Int32
      property terminating : Bool
      property compression_type : Int8
      property metrics : Bytes

      def initialize(@client_instance_id, @subscription_id, @terminating, @compression_type, @metrics)
      end

      def serialize(encoder : Encoder)
        encoder.io.write(@client_instance_id)
        encoder.write_int32(@subscription_id)
        encoder.write_boolean(@terminating)
        encoder.write_int8(@compression_type)
        encoder.write_compact_bytes(@metrics)
        encoder.write_tag_buffer
      end
    end

    struct PushTelemetryResponse
      property throttle_time_ms : Int32
      property error_code : Int16

      def initialize(@throttle_time_ms, @error_code)
      end

      def self.deserialize(decoder : Decoder) : PushTelemetryResponse
        throttle_time_ms = decoder.read_int32
        error_code = decoder.read_int16
        decoder.read_tag_buffer
        PushTelemetryResponse.new(throttle_time_ms, error_code)
      end
    end
  end
end
