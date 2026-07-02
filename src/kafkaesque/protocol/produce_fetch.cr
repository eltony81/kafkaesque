require "compress/gzip"
require "./crc32c"

module Kafkaesque
  module Protocol
    BUFFER_POOL = ObjectPool(IO::Memory).new { IO::Memory.new(4096) }

    alias BytesOrString = Bytes | String

    struct RecordHeader
      property key : String
      @value : Bytes

      def initialize(@key : String, value : BytesOrString)
        @value = value.is_a?(String) ? value.to_slice : value
      end

      def value : String
        String.new(@value)
      end

      def value_bytes : Bytes
        @value
      end
    end

    struct Record
      @key : Bytes?
      @value : Bytes?
      property headers : Array(RecordHeader)
      property partition : Int32 = 0
      property offset : Int64 = 0_i64
      property timestamp : Time? = nil

      def initialize(key : BytesOrString?, value : BytesOrString?, @headers = [] of RecordHeader, @partition = 0, @offset = 0_i64, @timestamp = nil)
        @key = key.is_a?(String) ? key.to_slice : key
        @value = value.is_a?(String) ? value.to_slice : value
      end

      def key : String?
        @key ? String.new(@key.not_nil!) : nil
      end

      def value : String?
        @value ? String.new(@value.not_nil!) : nil
      end

      def key_bytes : Bytes?
        @key
      end

      def value_bytes : Bytes?
        @value
      end

      def serialize(io : IO, first_timestamp_ms : Int64 = Time.utc.to_unix_ms, offset_delta : Int32 = 0)
        buffer = Protocol::BUFFER_POOL.rent
        buffer.clear
        begin
          encoder = Encoder.new(buffer)

          encoder.write_int8(0_i8) # attributes

          t_delta = 0_i64
          if ts = @timestamp
            t_delta = ts.to_unix_ms - first_timestamp_ms
          end
          encoder.write_varlong(t_delta)     # timestamp delta
          encoder.write_varint(offset_delta) # offset delta

          if (k_bytes = @key).nil?
            encoder.write_varint(-1)
          else
            encoder.write_varint(k_bytes.size)
            buffer.write(k_bytes)
          end

          if (v_bytes = @value).nil?
            encoder.write_varint(-1)
          else
            encoder.write_varint(v_bytes.size)
            buffer.write(v_bytes)
          end

          encoder.write_varint(@headers.size)
          @headers.each do |hdr|
            k_bytes = hdr.key.to_slice
            encoder.write_varint(k_bytes.size)
            buffer.write(k_bytes)

            v_bytes = hdr.value_bytes
            encoder.write_varint(v_bytes.size)
            buffer.write(v_bytes)
          end

          main_encoder = Encoder.new(io)
          main_encoder.write_varint(buffer.size.to_i32)
          io.write(buffer.to_slice)
        ensure
          Protocol::BUFFER_POOL.return(buffer)
        end
      end
    end

    struct RecordBatch
      property records : Array(Record)
      property producer_id : Int64
      property producer_epoch : Int16
      property base_sequence : Int32
      property compression : Int16 = 0_i16
      property base_offset : Int64 = 0_i64

      def initialize(@records, @producer_id = -1_i64, @producer_epoch = -1_i16, @base_sequence = -1, @compression = 0_i16, @base_offset = 0_i64)
      end

      def serialize(io : IO)
        encoder = Encoder.new(io)

        encoder.write_int64(@base_offset) # base offset
        batch_len_pos = encoder.reserve_int32

        batch_start_pos = io.pos

        encoder.write_int32(-1)  # partition leader epoch
        encoder.write_int8(2_i8) # magic byte

        crc_pos = encoder.reserve_uint32

        crc_start_pos = io.pos

        encoder.write_int16(@compression)      # attributes (lowest 3 bits define compression: 1 = GZIP)
        encoder.write_int32(@records.size - 1) # last offset delta

        first_t = @records.compact_map(&.timestamp).min? || Time.utc
        max_t = @records.compact_map(&.timestamp).max? || first_t
        first_timestamp_ms = first_t.to_unix_ms
        max_timestamp_ms = max_t.to_unix_ms

        encoder.write_int64(first_timestamp_ms) # first timestamp
        encoder.write_int64(max_timestamp_ms)   # max timestamp

        encoder.write_int64(@producer_id)    # producer id
        encoder.write_int16(@producer_epoch) # producer epoch
        encoder.write_int32(@base_sequence)  # base sequence

        encoder.write_int32(@records.size) # record count

        if @compression > 0_i16
          records_io = Protocol::BUFFER_POOL.rent
          records_io.clear
          begin
            @records.each_with_index do |record, idx|
              record.serialize(records_io, first_timestamp_ms, idx.to_i32)
            end
            compressed_bytes = Compression.compress(records_io.to_slice, @compression)
            io.write(compressed_bytes)
          ensure
            Protocol::BUFFER_POOL.return(records_io)
          end
        else
          @records.each_with_index do |record, idx|
            record.serialize(io, first_timestamp_ms, idx.to_i32)
          end
        end

        batch_end_pos = io.pos

        if io.is_a?(IO::Memory)
          crc_data = io.to_slice[crc_start_pos...batch_end_pos]
          crc = CRC32C.checksum(crc_data)
          encoder.patch_uint32(crc_pos, crc)

          batch_len = (batch_end_pos - batch_start_pos).to_i32
          encoder.patch_int32(batch_len_pos, batch_len)
        else
          raise "RecordBatch serialization requires a seekable IO::Memory"
        end
      end

      def self.deserialize_from_bytes(raw_bytes : Bytes, partition : Int32 = 0) : Array(Record)
        records = [] of Record
        io = IO::Memory.new(raw_bytes)
        dec = Decoder.new(io)

        while io.pos < io.size
          break if io.size - io.pos < 12

          base_offset = dec.read_int64
          batch_length = dec.read_int32
          break if io.size - io.pos < batch_length

          batch_data = io.to_slice[io.pos, batch_length]
          io.pos += batch_length

          batch_io = IO::Memory.new(batch_data)
          batch_dec = Decoder.new(batch_io)

          partition_leader_epoch = batch_dec.read_int32
          magic = batch_dec.read_int8
          crc = batch_dec.read_uint32
          attributes = batch_dec.read_int16
          last_offset_delta = batch_dec.read_int32
          first_timestamp = batch_dec.read_int64
          max_timestamp = batch_dec.read_int64
          producer_id = batch_dec.read_int64
          producer_epoch = batch_dec.read_int16
          base_sequence = batch_dec.read_int32
          record_count = batch_dec.read_int32

          codec = attributes & 0x07
          if codec > 0
            remaining = batch_data.size - batch_io.pos
            compressed_slice = batch_data[batch_io.pos, remaining]
            decompressed_bytes = Compression.decompress(compressed_slice, codec)
            batch_io = IO::Memory.new(decompressed_bytes)
            batch_dec = Decoder.new(batch_io)
          end

          record_count.times do
            break if batch_io.pos >= batch_io.size
            record_len = batch_dec.read_varint
            rec_attrs = batch_dec.read_int8
            timestamp_delta = batch_dec.read_varlong
            offset_delta = batch_dec.read_varint

            key_len = batch_dec.read_varint
            key_bytes = nil
            if key_len >= 0
              key_bytes = batch_io.to_slice[batch_io.pos, key_len]
              batch_io.pos += key_len
            end

            val_len = batch_dec.read_varint
            val_bytes = nil
            if val_len >= 0
              val_bytes = batch_io.to_slice[batch_io.pos, val_len]
              batch_io.pos += val_len
            end

            headers_count = batch_dec.read_varint
            rec_headers = [] of RecordHeader
            headers_count.times do
              h_key_len = batch_dec.read_varint
              h_key = ""
              if h_key_len > 0
                h_key_bytes = batch_io.to_slice[batch_io.pos, h_key_len]
                batch_io.pos += h_key_len
                h_key = String.new(h_key_bytes)
              end

              h_val_len = batch_dec.read_varint
              h_val_bytes = Bytes.empty
              if h_val_len > 0
                h_val_bytes = batch_io.to_slice[batch_io.pos, h_val_len]
                batch_io.pos += h_val_len
              end

              rec_headers << RecordHeader.new(h_key, h_val_bytes)
            end

            record_timestamp = Time.unix_ms(first_timestamp + timestamp_delta)
            records << Record.new(key_bytes, val_bytes, rec_headers, partition, base_offset + offset_delta.to_i64, record_timestamp)
          end
        end

        records
      end
    end

    struct ProduceRequest
      API_KEY     = 0_i16
      API_VERSION = 7_i16

      property acks : Int16
      property timeout_ms : Int32
      property topic : String
      property partition : Int32
      property records : Array(Record)
      property producer_id : Int64
      property producer_epoch : Int16
      property base_sequence : Int32
      property compression : Int16

      def initialize(@acks, @timeout_ms, @topic, @records, @partition = 0,
                     @producer_id = -1_i64, @producer_epoch = -1_i16, @base_sequence = -1, @compression = 0_i16)
      end

      def serialize(encoder : Encoder)
        encoder.write_string(nil) # transactional_id
        encoder.write_int16(@acks)
        encoder.write_int32(@timeout_ms)

        encoder.write_array([@topic]) do |topic_name|
          encoder.write_string(topic_name)
          encoder.write_array([@partition]) do |partition_idx|
            encoder.write_int32(partition_idx)

            batch_io = Protocol::BUFFER_POOL.rent
            batch_io.clear
            begin
              RecordBatch.new(
                @records,
                producer_id: @producer_id,
                producer_epoch: @producer_epoch,
                base_sequence: @base_sequence,
                compression: @compression
              ).serialize(batch_io)

              encoder.write_bytes(batch_io.to_slice)
            ensure
              Protocol::BUFFER_POOL.return(batch_io)
            end
          end
        end
      end
    end

    struct ProduceResponse
      property topic : String
      property partition : Int32
      property error_code : Int16
      property base_offset : Int64

      def initialize(@topic, @partition, @error_code, @base_offset)
      end

      def self.deserialize(decoder : Decoder) : ProduceResponse
        topic = ""
        partition = 0
        error_code = 0_i16
        base_offset = 0_i64

        decoder.read_array do
          topic = decoder.read_string.to_s
          decoder.read_array do
            partition = decoder.read_int32
            error_code = decoder.read_int16
            base_offset = decoder.read_int64
            decoder.read_int64 # log append time
            decoder.read_int64 # log start offset
          end
        end
        decoder.read_int32 # throttle_time_ms

        ProduceResponse.new(topic, partition, error_code, base_offset)
      end
    end

    struct FetchRequest
      API_KEY     =  1_i16
      API_VERSION = 11_i16 # v11: adds RackId (KIP-392), still non-flexible (flexibleVersions start at 12)

      property topic : String
      property partition : Int32
      property fetch_offset : Int64
      property max_bytes : Int32
      property min_bytes : Int32
      property partition_max_bytes : Int32
      property rack_id : String?
      property current_leader_epoch : Int32

      def initialize(@topic, @partition, @fetch_offset, @min_bytes = 1, @max_bytes = 1048576, @partition_max_bytes = 1048576, @rack_id = nil, @current_leader_epoch = -1)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(-1)         # replica_id
        encoder.write_int32(1000)       # max_wait_ms
        encoder.write_int32(@min_bytes) # min_bytes
        encoder.write_int32(@max_bytes) # max_bytes
        encoder.write_int8(0_i8)        # isolation_level
        encoder.write_int32(0)          # session_id
        encoder.write_int32(-1)         # session_epoch

        encoder.write_array([@topic]) do |topic_name|
          encoder.write_string(topic_name)
          encoder.write_array([@partition]) do |part_idx|
            encoder.write_int32(part_idx)
            encoder.write_int32(@current_leader_epoch) # (KIP-320) fences requests against a stale leader
            encoder.write_int64(@fetch_offset)         # fetch_offset
            encoder.write_int64(-1_i64)                # log_start_offset
            encoder.write_int32(@partition_max_bytes)  # partition_max_bytes
          end
        end

        encoder.write_array([] of String) { } # forgotten_topics_data (no incremental fetch sessions)
        encoder.write_string(@rack_id || "")  # rack_id (KIP-392): enables broker-side follower-read eligibility
      end
    end

    struct FetchResponse
      property error_code : Int16
      property records : Array(Record)
      property preferred_read_replica : Int32

      def initialize(@error_code, @records, @preferred_read_replica = -1)
      end

      def self.deserialize(decoder : Decoder) : FetchResponse
        decoder.read_int32 # throttle_time_ms
        decoder.read_int16 # top-level error_code (v7+)
        decoder.read_int32 # session_id (v7+)
        error_code = 0_i16
        records = [] of Record
        preferred_read_replica = -1

        decoder.read_array do
          topic = decoder.read_string
          decoder.read_array do
            partition_idx = decoder.read_int32
            error_code = decoder.read_int16
            high_watermark = decoder.read_int64
            last_stable_offset = decoder.read_int64
            log_start_offset = decoder.read_int64 # v5+

            decoder.read_array do
              decoder.read_int64 # producer_id
              decoder.read_int64 # first_offset
            end

            preferred_read_replica = decoder.read_int32 # v11+

            raw_bytes = decoder.read_bytes
            if !raw_bytes.nil? && !raw_bytes.empty?
              records = RecordBatch.deserialize_from_bytes(raw_bytes, partition: partition_idx)
            end
          end
        end

        FetchResponse.new(error_code, records, preferred_read_replica)
      end
    end

    # A single partition's fetch parameters within a FetchSessionRequest.
    struct FetchPartitionSpec
      property partition : Int32
      property fetch_offset : Int64
      property current_leader_epoch : Int32

      def initialize(@partition, @fetch_offset, @current_leader_epoch = -1)
      end
    end

    # KIP-227 incremental fetch sessions: multiplexes every partition of a
    # single topic routed to one broker connection into a single request,
    # tracked by (session_id, session_epoch) rather than one FetchRequest per
    # partition. `partitions` carries only the partitions being added or
    # whose fetch_offset changed since the last request on this session —
    # the broker remembers the rest. `forgotten_partitions` removes
    # partitions from an already-open session (e.g. after a rebalance
    # revokes them). session_epoch: 0 opens/resets a session, -1 closes one,
    # anything else continues it. See Client#fetch_many.
    struct FetchSessionRequest
      API_KEY     =  1_i16
      API_VERSION = 11_i16

      property topic : String
      property partitions : Array(FetchPartitionSpec)
      property session_id : Int32
      property session_epoch : Int32
      property forgotten_partitions : Array(Int32)
      property max_bytes : Int32
      property min_bytes : Int32
      property partition_max_bytes : Int32
      property rack_id : String?

      def initialize(@topic, @partitions, @session_id = 0, @session_epoch = 0, @forgotten_partitions = [] of Int32,
                     @min_bytes = 1, @max_bytes = 1048576, @partition_max_bytes = 1048576, @rack_id = nil)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(-1)         # replica_id
        encoder.write_int32(1000)       # max_wait_ms
        encoder.write_int32(@min_bytes) # min_bytes
        encoder.write_int32(@max_bytes) # max_bytes
        encoder.write_int8(0_i8)        # isolation_level
        encoder.write_int32(@session_id)
        encoder.write_int32(@session_epoch)

        if @partitions.empty?
          encoder.write_array([] of String) { }
        else
          encoder.write_array([@topic]) do |topic_name|
            encoder.write_string(topic_name)
            encoder.write_array(@partitions) do |spec|
              encoder.write_int32(spec.partition)
              encoder.write_int32(spec.current_leader_epoch)
              encoder.write_int64(spec.fetch_offset)
              encoder.write_int64(-1_i64) # log_start_offset
              encoder.write_int32(@partition_max_bytes)
            end
          end
        end

        if @forgotten_partitions.empty?
          encoder.write_array([] of String) { }
        else
          encoder.write_array([@topic]) do |topic_name|
            encoder.write_string(topic_name)
            encoder.write_array(@forgotten_partitions) { |p| encoder.write_int32(p) }
          end
        end

        encoder.write_string(@rack_id || "")
      end
    end

    struct FetchSessionPartitionResult
      property error_code : Int16
      property records : Array(Record)
      property preferred_read_replica : Int32

      def initialize(@error_code, @records, @preferred_read_replica = -1)
      end
    end

    struct FetchSessionResponse
      property error_code : Int16
      property session_id : Int32
      # partition_index => result. Only partitions the broker actually had
      # something to report on for this response are present — with an
      # established session, that can be a subset of the full session.
      property partitions : Hash(Int32, FetchSessionPartitionResult)

      def initialize(@error_code, @session_id, @partitions)
      end

      def self.deserialize(decoder : Decoder) : FetchSessionResponse
        decoder.read_int32 # throttle_time_ms
        error_code = decoder.read_int16
        session_id = decoder.read_int32
        partitions = {} of Int32 => FetchSessionPartitionResult

        decoder.read_array do
          decoder.read_string # topic
          decoder.read_array do
            partition_idx = decoder.read_int32
            part_error_code = decoder.read_int16
            decoder.read_int64 # high_watermark
            decoder.read_int64 # last_stable_offset
            decoder.read_int64 # log_start_offset

            decoder.read_array do
              decoder.read_int64 # producer_id
              decoder.read_int64 # first_offset
            end

            preferred_read_replica = decoder.read_int32

            raw_bytes = decoder.read_bytes
            records = if !raw_bytes.nil? && !raw_bytes.empty?
                        RecordBatch.deserialize_from_bytes(raw_bytes, partition: partition_idx)
                      else
                        [] of Record
                      end

            partitions[partition_idx] = FetchSessionPartitionResult.new(part_error_code, records, preferred_read_replica)
          end
        end

        FetchSessionResponse.new(error_code, session_id, partitions)
      end
    end

    # -----------------------------------------------------------------------
    # ListOffsets protocol (v1) — used to find earliest/latest partition offsets
    # timestamp: -2 = earliest, -1 = latest
    # -----------------------------------------------------------------------
    struct ListOffsetsRequest
      API_KEY     = 2_i16
      API_VERSION = 1_i16

      def initialize(@topic : String, @partition : Int32, @timestamp : Int64)
      end

      def serialize(encoder : Encoder)
        encoder.write_int32(-1) # replica_id (consumer = -1)
        encoder.write_array([@topic]) do |topic_name|
          encoder.write_string(topic_name)
          encoder.write_array([@partition]) do |part|
            encoder.write_int32(part)
            encoder.write_int64(@timestamp)
          end
        end
      end
    end

    struct ListOffsetsResponse
      property error_code : Int16
      property offset : Int64

      def initialize(@error_code, @offset)
      end

      def self.deserialize(decoder : Decoder) : ListOffsetsResponse
        error_code = 0_i16
        offset = -1_i64
        decoder.read_array do
          _topic = decoder.read_string
          decoder.read_array do
            _partition = decoder.read_int32
            error_code = decoder.read_int16
            _timestamp = decoder.read_int64 # v1: timestamp field
            offset = decoder.read_int64
          end
        end
        ListOffsetsResponse.new(error_code, offset)
      end
    end
  end
end
