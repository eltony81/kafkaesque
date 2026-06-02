module Kafkaesque
  module Protocol
    struct SaslHandshakeRequest
      API_KEY = 17_i16
      API_VERSION = 1_i16

      property mechanism : String

      def initialize(@mechanism)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(@mechanism)
      end
    end

    struct SaslHandshakeResponse
      property error_code : Int16
      property mechanisms : Array(String)

      def initialize(@error_code, @mechanisms)
      end

      def self.deserialize(decoder : Decoder) : SaslHandshakeResponse
        error_code = decoder.read_int16
        mechanisms = decoder.read_array { decoder.read_string.to_s } || [] of String
        SaslHandshakeResponse.new(error_code, mechanisms)
      end
    end

    struct SaslAuthenticateRequest
      API_KEY = 36_i16
      API_VERSION = 1_i16

      property auth_bytes : Bytes

      def initialize(@auth_bytes)
      end

      def serialize(encoder : Encoder)
        encoder.write_bytes(@auth_bytes)
      end

      # OAUTHBEARER helper to format the token string matching RFC 7628
      def self.oauthbearer_payload(token : String, host : String, port : Int32) : Bytes
        payload_str = "n,,\u0001auth=Bearer #{token}\u0001host=#{host}\u0001port=#{port}\u0001\u0001"
        payload_str.to_slice
      end
    end

    struct SaslAuthenticateResponse
      property error_code : Int16
      property error_message : String?
      property auth_bytes : Bytes?

      def initialize(@error_code, @error_message, @auth_bytes)
      end

      def self.deserialize(decoder : Decoder) : SaslAuthenticateResponse
        error_code = decoder.read_int16
        error_message = decoder.read_string
        auth_bytes = decoder.read_bytes
        SaslAuthenticateResponse.new(error_code, error_message, auth_bytes)
      end
    end
  end
end
