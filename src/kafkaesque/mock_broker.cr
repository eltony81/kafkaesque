require "socket"
require "./protocol/types"
require "./protocol/request"

module Kafkaesque
  class MockBroker
    getter port : Int32
    @server : TCPServer
    @running = true
    @handlers = {} of Int16 => Proc(Protocol::Decoder, Int16, IO::Memory)

    # Failure & Latency simulation properties
    property latency_ms : Int32 = 0
    property drop_after_requests : Int32? = nil

    # Stateful offset commit tracking
    @committed_offsets = {} of String => Int64
    @offset_mutex = Mutex.new

    def initialize
      @server = TCPServer.new("127.0.0.1", 0)
      @port = @server.local_address.port
      spawn_server_loop
    end

    def on_request(api_key : Int16, &block : Protocol::Decoder, Int16 -> IO::Memory)
      @handlers[api_key] = block
    end

    def close
      @running = false
      @server.close rescue nil
    end

    private def spawn_server_loop
      spawn do
        while @running
          begin
            client_socket = @server.accept
            spawn handle_client(client_socket)
          rescue
            break unless @running
          end
        end
      end
    end

    private def handle_client(socket)
      request_count = 0
      loop do
        break if socket.closed?
        begin
          size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian) rescue nil
          break if size.nil? || size <= 0

          buf = Bytes.new(size)
          socket.read_fully(buf)

          request_count += 1
          if max_reqs = @drop_after_requests
            if request_count >= max_reqs
              socket.close rescue nil
              break
            end
          end

          mem = IO::Memory.new(buf)
          decoder = Protocol::Decoder.new(mem)

          api_key = decoder.read_int16
          api_version = decoder.read_int16
          correlation_id = decoder.read_int32

          # Check if request has a flexible header
          flexible_request = (api_key == 18_i16 && api_version >= 3_i16) || (api_key == 84_i16 && api_version >= 1_i16) || (api_key == 78_i16) || (api_key == 79_i16) || (api_key == 85_i16) || (api_key == 71_i16) || (api_key == 72_i16) || (api_key == 8_i16 && api_version >= 9_i16)
          flexible_response = flexible_request && (api_key != 18_i16)

          client_id = decoder.read_string
          if flexible_request
            decoder.read_varint # consume RequestHeader tag buffer
          end

          response_body_io = IO::Memory.new
          if handler = @handlers[api_key]?
            body_mem = handler.call(decoder, api_version)
            response_body_io.write(body_mem.to_slice)
          elsif api_key == 8_i16
            # Default stateful OffsetCommit handling
            group_id = decoder.read_compact_string
            generation_id = decoder.read_int32
            member_id = decoder.read_compact_string
            group_instance_id = decoder.read_compact_string

            topic = ""
            partition = 0
            offset = -1_i64

            decoder.read_compact_array do
              topic = decoder.read_compact_string.to_s
              decoder.read_compact_array do
                partition = decoder.read_int32
                offset = decoder.read_int64
                committed_leader_epoch = decoder.read_int32
                metadata = decoder.read_compact_string
                decoder.read_tag_buffer

                @offset_mutex.synchronize do
                  @committed_offsets["#{group_id}:#{topic}:#{partition}"] = offset
                end
              end
              decoder.read_tag_buffer
            end
            decoder.read_tag_buffer

            # Write response
            enc = Protocol::Encoder.new(response_body_io)
            enc.write_int32(0) # throttle_time_ms
            enc.write_compact_array([topic]) do |t|
              enc.write_compact_string(t)
              enc.write_compact_array([partition]) do |p|
                enc.write_int32(p)
                enc.write_int16(0_i16) # error_code
                enc.write_tag_buffer
              end
              enc.write_tag_buffer
            end
            enc.write_tag_buffer
          elsif api_key == 9_i16
            # Default stateful OffsetFetch handling
            group_id = decoder.read_string.to_s
            topic = ""
            partition = 0

            decoder.read_array do
              topic = decoder.read_string.to_s
              decoder.read_array do
                partition = decoder.read_int32
              end
            end

            offset = -1_i64
            @offset_mutex.synchronize do
              offset = @committed_offsets["#{group_id}:#{topic}:#{partition}"]? || -1_i64
            end

            # Write response
            enc = Protocol::Encoder.new(response_body_io)
            enc.write_int32(0) # throttle_time_ms
            enc.write_array([topic]) do |t|
              enc.write_string(t)
              enc.write_array([partition]) do |p|
                enc.write_int32(p)
                enc.write_int64(offset)
                enc.write_string(nil)  # metadata
                enc.write_int16(0_i16) # error_code
              end
            end
          elsif api_key == 18_i16
            # Default ApiVersions response
            enc = Protocol::Encoder.new(response_body_io)
            enc.write_int16(0_i16) # error_code

            # Mock some keys: Produce(0), Fetch(1), ListOffsets(2), Metadata(3), OffsetCommit(8), OffsetFetch(9), ApiVersions(18)
            keys = [0_i16, 1_i16, 2_i16, 3_i16, 8_i16, 9_i16, 18_i16]
            enc.write_compact_array(keys) do |k|
              enc.write_int16(k)
              enc.write_int16(0_i16)
              enc.write_int16(k == 8_i16 ? 9_i16 : (k == 9_i16 ? 3_i16 : 7_i16))
              enc.write_tag_buffer
            end
            enc.write_int32(0) # throttle_time_ms
            enc.write_tag_buffer
          else
            # Default response: just error code (0)
            response_body_io.write_bytes(0_i16, IO::ByteFormat::BigEndian)
          end

          # Simulate response latency
          if @latency_ms > 0
            sleep @latency_ms.milliseconds
          end

          resp_mem = IO::Memory.new
          if flexible_response
            resp_size = 4 + 1 + response_body_io.size
            resp_mem.write_bytes(resp_size.to_i32, IO::ByteFormat::BigEndian)
            resp_mem.write_bytes(correlation_id.to_i32, IO::ByteFormat::BigEndian)
            resp_mem.write_byte(0_u8) # ResponseHeader tag buffer
          else
            resp_size = 4 + response_body_io.size
            resp_mem.write_bytes(resp_size.to_i32, IO::ByteFormat::BigEndian)
            resp_mem.write_bytes(correlation_id.to_i32, IO::ByteFormat::BigEndian)
          end
          resp_mem.write(response_body_io.to_slice)

          socket.write(resp_mem.to_slice)
          socket.flush
        rescue ex
          break
        end
      end
    ensure
      socket.close rescue nil
    end
  end
end
