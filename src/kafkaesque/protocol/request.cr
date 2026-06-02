module Kafkaesque
  module Protocol
    struct RequestHeader
      property api_key : Int16
      property api_version : Int16
      property correlation_id : Int32
      property client_id : String?
      property flexible : Bool

      def initialize(@api_key, @api_version, @correlation_id, @client_id, @flexible = false)
      end

      def serialize(encoder : Encoder)
        encoder.write_int16(@api_key)
        encoder.write_int16(@api_version)
        encoder.write_int32(@correlation_id)
        encoder.write_string(@client_id)
        if @flexible
          # Write empty tag buffer (varint 0)
          encoder.write_varint(0)
        end
      end
    end
  end
end
