module Kafkaesque
  module Protocol
    module CRC32C
      POLY = 0x82F63B78_u32

      TABLE = StaticArray(UInt32, 256).new do |i|
        crc = i.to_u32
        8.times do
          if (crc & 1) != 0
            crc = (crc >> 1) ^ POLY
          else
            crc >>= 1
          end
        end
        crc
      end

      def self.checksum(data : Bytes) : UInt32
        crc = 0xFFFFFFFF_u32
        data.each do |byte|
          crc = TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
        end
        crc ^ 0xFFFFFFFF_u32
      end
    end
  end
end
