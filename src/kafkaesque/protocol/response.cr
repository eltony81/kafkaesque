module Kafkaesque
  module Protocol
    struct ResponseHeader
      property correlation_id : Int32
      property flexible : Bool

      def initialize(@correlation_id, @flexible = false)
      end

      def self.deserialize(decoder : Decoder, flexible : Bool) : ResponseHeader
        correlation_id = decoder.read_int32
        if flexible
          tag_count = decoder.read_varint
          if tag_count > 0
            tag_count.times do
              _tag_id = decoder.read_varint
              tag_len = decoder.read_varint
              decoder.io.skip(tag_len)
            end
          end
        end
        ResponseHeader.new(correlation_id, flexible)
      end
    end
  end
end
