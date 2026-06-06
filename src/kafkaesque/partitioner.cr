module Kafkaesque
  module Partitioner
    abstract class Base
      abstract def partition(topic : String, key : Bytes?, value : Bytes?, partitions_count : Int32) : Int32
    end

    class MurmurHash2 < Base
      SEED = 0x9747b28c_u32
      M    = 0x5bd1e995_u32
      R    =             24

      def partition(topic : String, key : Bytes?, value : Bytes?, partitions_count : Int32) : Int32
        return 0 if partitions_count <= 0
        return 0 if key.nil? || key.empty?

        h = SEED ^ key.size
        len = key.size
        len_4 = len // 4

        len_4.times do |i|
          offset = i * 4
          k = (key[offset].to_u32 & 0xff) |
              ((key[offset + 1].to_u32 & 0xff) << 8) |
              ((key[offset + 2].to_u32 & 0xff) << 16) |
              ((key[offset + 3].to_u32 & 0xff) << 24)

          k = (k &* M)
          k ^= (k >> R)
          k = (k &* M)

          h = (h &* M)
          h ^= k
        end

        extra = len % 4
        tail = len & ~3
        if extra >= 1
          k_extra = 0_u32
          if extra >= 3
            k_extra ^= (key[tail + 2].to_u32 & 0xff) << 16
          end
          if extra >= 2
            k_extra ^= (key[tail + 1].to_u32 & 0xff) << 8
          end
          k_extra ^= (key[tail].to_u32 & 0xff)
          h ^= k_extra
          h = (h &* M)
        end

        h ^= (h >> 13)
        h = (h &* M)
        h ^= (h >> 15)

        (h.to_i32! & 0x7fffffff) % partitions_count
      end
    end

    class RoundRobin < Base
      @index = 0_u32
      @lock = Mutex.new

      def partition(topic : String, key : Bytes?, value : Bytes?, partitions_count : Int32) : Int32
        return 0 if partitions_count <= 0
        idx = 0_u32
        @lock.synchronize do
          idx = @index
          @index = @index &+ 1
        end
        (idx % partitions_count).to_i
      end
    end
  end
end
