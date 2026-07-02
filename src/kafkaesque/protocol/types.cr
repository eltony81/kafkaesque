module Kafkaesque
  module Protocol
    struct Encoder
      getter io : IO

      def initialize(@io : IO)
      end

      def write_int8(val : Int8)
        @io.write_byte(val.to_u8!)
      end

      def write_int16(val : Int16)
        @io.write_bytes(val, IO::ByteFormat::BigEndian)
      end

      def write_int32(val : Int32)
        @io.write_bytes(val, IO::ByteFormat::BigEndian)
      end

      def write_uint32(val : UInt32)
        @io.write_bytes(val, IO::ByteFormat::BigEndian)
      end

      def write_int64(val : Int64)
        @io.write_bytes(val, IO::ByteFormat::BigEndian)
      end

      def write_varint(val : Int32)
        write_varlong(val.to_i64)
      end

      def write_varlong(val : Int64)
        # ZigZag encode (signed)
        uval = (val << 1) ^ (val >> 63)
        write_uvarint64(uval.to_u64)
      end

      # Unsigned varint (used by Kafka flexible encoding for compact lengths)
      def write_uvarint(val : Int32)
        write_uvarint64(val.to_u64)
      end

      def write_uvarint64(val : UInt64)
        loop do
          temp = val & 0x7f
          val >>= 7
          if val != 0
            @io.write_byte((temp | 0x80).to_u8)
          else
            @io.write_byte(temp.to_u8)
            break
          end
        end
      end

      # Empty tagged fields section (flexible version trailer)
      def write_tag_buffer
        write_uvarint(0) # tag count = 0
      end

      def write_string(val : String?)
        if val.nil?
          write_int16(-1)
        else
          bytes = val.to_slice
          write_int16(bytes.size.to_i16)
          @io.write(bytes)
        end
      end

      def write_compact_string(val : String?)
        if val.nil?
          write_uvarint(0) # null compact string
        else
          bytes = val.to_slice
          write_uvarint(bytes.size + 1)
          @io.write(bytes)
        end
      end

      def write_bytes(val : Bytes?)
        if val.nil?
          write_int32(-1)
        else
          write_int32(val.size)
          @io.write(val)
        end
      end

      def write_compact_bytes(val : Bytes?)
        if val.nil?
          write_uvarint(0) # null compact bytes
        else
          write_uvarint(val.size + 1)
          @io.write(val)
        end
      end

      def write_array(array : Array(T)?, &) forall T
        if array.nil?
          write_int32(-1)
        else
          write_int32(array.size)
          array.each do |item|
            yield item
          end
        end
      end

      def write_compact_array(array : Array(T)?, &) forall T
        if array.nil?
          write_uvarint(0) # null compact array
        else
          write_uvarint(array.size + 1)
          array.each do |item|
            yield item
          end
        end
      end

      def write_boolean(val : Bool)
        write_int8(val ? 1_i8 : 0_i8)
      end

      def reserve_int32 : Int64
        pos = @io.pos.to_i64
        write_int32(0)
        pos
      end

      def patch_int32(pos : Int64, val : Int32)
        current = @io.pos.to_i64
        @io.seek(pos)
        write_int32(val)
        @io.seek(current)
      end

      def reserve_uint32 : Int64
        pos = @io.pos.to_i64
        write_uint32(0_u32)
        pos
      end

      def patch_uint32(pos : Int64, val : UInt32)
        current = @io.pos.to_i64
        @io.seek(pos)
        write_uint32(val)
        @io.seek(current)
      end
    end

    struct Decoder
      getter io : IO

      def initialize(@io : IO)
      end

      def read_int8 : Int8
        val = @io.read_byte
        raise IO::EOFError.new if val.nil?
        val.to_i8!
      end

      def read_int16 : Int16
        @io.read_bytes(Int16, IO::ByteFormat::BigEndian)
      end

      def read_int32 : Int32
        @io.read_bytes(Int32, IO::ByteFormat::BigEndian)
      end

      def read_uint32 : UInt32
        @io.read_bytes(UInt32, IO::ByteFormat::BigEndian)
      end

      def read_int64 : Int64
        @io.read_bytes(Int64, IO::ByteFormat::BigEndian)
      end

      def read_varint : Int32
        read_varlong.to_i32
      end

      def read_varlong : Int64
        uval = 0_u64
        shift = 0
        loop do
          b = @io.read_byte
          raise IO::EOFError.new if b.nil?
          uval |= ((b & 0x7f).to_u64 << shift)
          break if (b & 0x80) == 0
          shift += 7
          if shift >= 64
            raise "Varint too long"
          end
        end
        # ZigZag decode
        (uval >> 1).to_i64 ^ -((uval & 1).to_i64)
      end

      # Unsigned varint (used by Kafka flexible encoding)
      def read_uvarint : Int32
        read_uvarint64.to_i32
      end

      def read_uvarint64 : UInt64
        uval = 0_u64
        shift = 0
        loop do
          b = @io.read_byte
          raise IO::EOFError.new if b.nil?
          uval |= ((b & 0x7f).to_u64 << shift)
          break if (b & 0x80) == 0
          shift += 7
          raise "Uvarint too long" if shift >= 64
        end
        uval
      end

      # Skip the tagged fields section present in all flexible-version responses
      def read_tag_buffer
        count = read_uvarint
        count.times do
          _tag_id = read_uvarint # field tag
          tag_len = read_uvarint # field byte length
          buf = Bytes.new(tag_len)
          @io.read_fully(buf)
        end
      end

      def read_string : String?
        len = read_int16
        return nil if len == -1
        raise "Negative string length: #{len}" if len < -1
        buf = Bytes.new(len)
        @io.read_fully(buf)
        String.new(buf)
      end

      def read_compact_string : String?
        len = read_uvarint
        return nil if len == 0 # 0 = null in compact encoding
        actual_len = len - 1
        return "" if actual_len == 0
        buf = Bytes.new(actual_len)
        @io.read_fully(buf)
        String.new(buf)
      end

      def read_bytes : Bytes?
        len = read_int32
        return nil if len == -1
        raise "Negative bytes length: #{len}" if len < -1
        buf = Bytes.new(len)
        @io.read_fully(buf)
        buf
      end

      def read_compact_bytes : Bytes?
        len = read_uvarint
        return nil if len == 0 # 0 = null in compact encoding
        actual_len = len - 1
        return Bytes.empty if actual_len == 0
        buf = Bytes.new(actual_len)
        @io.read_fully(buf)
        buf
      end

      def read_array(&block : -> T) : Array(T)? forall T
        len = read_int32
        return nil if len == -1
        raise "Negative array length: #{len}" if len < -1
        Array(T).new(len) { block.call }
      end

      def read_compact_array(&block : -> T) : Array(T)? forall T
        len = read_uvarint
        return nil if len == 0 # 0 = null in compact encoding
        actual_len = len - 1
        Array(T).new(actual_len) { block.call }
      end

      def read_boolean : Bool
        read_int8 != 0
      end
    end
  end
end
