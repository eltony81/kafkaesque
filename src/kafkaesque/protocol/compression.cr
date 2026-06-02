module Kafkaesque
  module Protocol
    module Compression
      @[Link("snappy")]
      lib LibSnappy
        fun compress = snappy_compress(input : UInt8*, input_length : LibC::SizeT, compressed : UInt8*, compressed_length : LibC::SizeT*) : Int32
        fun uncompress = snappy_uncompress(compressed : UInt8*, compressed_length : LibC::SizeT, uncompressed : UInt8*, uncompressed_length : LibC::SizeT*) : Int32
        fun max_compressed_length = snappy_max_compressed_length(input_length : LibC::SizeT) : LibC::SizeT
        fun uncompressed_length = snappy_uncompressed_length(compressed : UInt8*, compressed_length : LibC::SizeT, result : LibC::SizeT*) : Int32
      end

      @[Link("zstd")]
      lib LibZstd
        fun compress = ZSTD_compress(dst : UInt8*, dstCapacity : LibC::SizeT, src : UInt8*, srcSize : LibC::SizeT, compressionLevel : Int32) : LibC::SizeT
        fun decompress = ZSTD_decompress(dst : UInt8*, dstCapacity : LibC::SizeT, src : UInt8*, srcSize : LibC::SizeT) : LibC::SizeT
        fun getFrameContentSize = ZSTD_getFrameContentSize(src : UInt8*, srcSize : LibC::SizeT) : UInt64
        fun compressBound = ZSTD_compressBound(srcSize : LibC::SizeT) : LibC::SizeT
        fun isError = ZSTD_isError(code : LibC::SizeT) : UInt32
      end

      @[Link("lz4")]
      lib LibLZ4
        fun compress_frame = LZ4F_compressFrame(dstBuffer : UInt8*, dstCapacity : LibC::SizeT, srcBuffer : UInt8*, srcSize : LibC::SizeT, preferences : Void*) : LibC::SizeT
        fun isError = LZ4F_isError(code : LibC::SizeT) : UInt32
        fun createDecompressionContext = LZ4F_createDecompressionContext(dctxPtr : Void**, version : UInt32) : LibC::SizeT
        fun freeDecompressionContext = LZ4F_freeDecompressionContext(dctx : Void*) : LibC::SizeT
        fun decompress = LZ4F_decompress(dctx : Void*, dstBuffer : UInt8*, dstSizePtr : LibC::SizeT*, srcBuffer : UInt8*, srcSizePtr : LibC::SizeT*, options : Void*) : LibC::SizeT
      end

      def self.compress(data : Bytes, codec : Int16) : Bytes
        case codec
        when 1 # GZIP
          io = IO::Memory.new
          Compress::Gzip::Writer.open(io) do |gzip|
            gzip.write(data)
          end
          io.to_slice
        when 2 # Snappy
          bound = LibSnappy.max_compressed_length(data.size.to_u64)
          dest = Bytes.new(bound)
          comp_len = bound.to_u64
          status = LibSnappy.compress(data, data.size.to_u64, dest, pointerof(comp_len))
          raise "Snappy compression failed with status #{status}" if status != 0
          dest[0, comp_len]
        when 3 # LZ4
          # Standard frame format compression bound is frame size (~compBound + header)
          # A safe bound is twice data size + 64K
          bound = data.size.to_u64 * 2 + 65536
          dest = Bytes.new(bound)
          comp_len = LibLZ4.compress_frame(dest, bound, data, data.size.to_u64, nil)
          raise "LZ4 compression failed" if LibLZ4.isError(comp_len) != 0
          dest[0, comp_len]
        when 4 # Zstd
          bound = LibZstd.compressBound(data.size.to_u64)
          dest = Bytes.new(bound)
          res = LibZstd.compress(dest, bound, data, data.size.to_u64, 3)
          raise "Zstd compression failed" if LibZstd.isError(res) != 0
          dest[0, res]
        else
          data
        end
      end

      def self.decompress(data : Bytes, codec : Int16) : Bytes
        case codec
        when 1 # GZIP
          io = IO::Memory.new
          Compress::Gzip::Reader.open(IO::Memory.new(data)) do |gzip|
            IO.copy(gzip, io)
          end
          io.to_slice
        when 2 # Snappy
          status_len = LibSnappy.uncompressed_length(data, data.size.to_u64, out uncompressed_len)
          raise "Snappy uncompressed length check failed (status: #{status_len})" if status_len != 0
          
          dest = Bytes.new(uncompressed_len)
          decomp_len = uncompressed_len.to_u64
          status = LibSnappy.uncompress(data, data.size.to_u64, dest, pointerof(decomp_len))
          raise "Snappy decompression failed (status: #{status})" if status != 0
          dest[0, decomp_len]
        when 3 # LZ4
          status = LibLZ4.createDecompressionContext(out dctx, 100_u32)
          raise "Failed to create LZ4 decompression context" if LibLZ4.isError(status) != 0
          
          begin
            capacity = 1024 * 1024
            dest = Bytes.new(capacity)
            src_pos = 0_u64
            dest_pos = 0_u64
            
            loop do
              src_size = data.size.to_u64 - src_pos
              dest_size = dest.size.to_u64 - dest_pos
              
              src_ptr = data.to_unsafe + src_pos
              dest_ptr = dest.to_unsafe + dest_pos
              
              res = LibLZ4.decompress(dctx, dest_ptr, pointerof(dest_size), src_ptr, pointerof(src_size), nil)
              raise "LZ4 decompression error" if LibLZ4.isError(res) != 0
              
              src_pos += src_size
              dest_pos += dest_size
              
              break if res == 0
              if src_pos >= data.size
                raise "LZ4 decompression ended prematurely"
              end
              
              if dest_pos >= dest.size
                capacity *= 2
                new_dest = Bytes.new(capacity)
                dest.copy_to(new_dest)
                dest = new_dest
              end
            end
            dest[0, dest_pos]
          ensure
            LibLZ4.freeDecompressionContext(dctx)
          end
        when 4 # Zstd
          size = LibZstd.getFrameContentSize(data, data.size.to_u64)
          if size == 0xFFFFFFFFFFFFFFFF_u64 || size == 0xFFFFFFFFFFFFFFFE_u64
            capacity = 1024 * 1024
            dest = Bytes.new(capacity)
            loop do
              res = LibZstd.decompress(dest, capacity.to_u64, data, data.size.to_u64)
              if LibZstd.isError(res) == 0
                return dest[0, res]
              end
              capacity *= 2
              dest = Bytes.new(capacity)
              break if capacity > 50 * 1024 * 1024
            end
            raise "Zstd decompression failed"
          else
            dest = Bytes.new(size)
            res = LibZstd.decompress(dest, size, data, data.size.to_u64)
            raise "Zstd decompression failed" if LibZstd.isError(res) != 0
            dest[0, res]
          end
        else
          data
        end
      end
    end
  end
end
