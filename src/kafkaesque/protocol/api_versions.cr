module Kafkaesque
  module Protocol
    struct ApiVersionInfo
      property api_key : Int16
      property min_version : Int16
      property max_version : Int16

      def initialize(@api_key, @min_version, @max_version)
      end

      def self.deserialize(decoder : Decoder) : ApiVersionInfo
        api_key = decoder.read_int16
        min_version = decoder.read_int16
        max_version = decoder.read_int16
        decoder.read_tag_buffer
        ApiVersionInfo.new(api_key, min_version, max_version)
      end
    end

    struct ApiVersionsRequest
      API_KEY     = 18_i16
      API_VERSION =  3_i16

      property client_software_name : String
      property client_software_version : String

      def initialize(@client_software_name = "kafkaesque", @client_software_version = Kafkaesque::VERSION)
      end

      def serialize(encoder : Encoder)
        encoder.write_compact_string(@client_software_name)
        encoder.write_compact_string(@client_software_version)
        encoder.write_tag_buffer
      end
    end

    struct ApiVersionsResponse
      property error_code : Int16
      property api_keys : Array(ApiVersionInfo)
      property throttle_time_ms : Int32

      def initialize(@error_code, @api_keys, @throttle_time_ms = 0)
      end

      def self.deserialize(decoder : Decoder) : ApiVersionsResponse
        error_code = decoder.read_int16
        api_keys = decoder.read_compact_array { ApiVersionInfo.deserialize(decoder) } || [] of ApiVersionInfo
        throttle_time_ms = decoder.read_int32
        decoder.read_tag_buffer
        ApiVersionsResponse.new(error_code, api_keys, throttle_time_ms)
      end
    end
  end
end
