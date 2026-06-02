require "socket"
require "openssl"

module Kafkaesque
  class Connection
    getter host : String
    getter port : Int32
    getter use_ssl : Bool
    @socket : TCPSocket | OpenSSL::SSL::Socket::Client
    @write_channel : Channel(Tuple(Bytes, Channel(Nil)))

    def initialize(@host : String, @port : Int32, @use_ssl : Bool = false, context : OpenSSL::SSL::Context::Client? = nil)
      tcp = TCPSocket.new(@host, @port)
      tcp.tcp_nodelay = true
      if @use_ssl
        ctx = context || OpenSSL::SSL::Context::Client.new
        @socket = OpenSSL::SSL::Socket::Client.new(tcp, ctx)
      else
        @socket = tcp
      end

      @write_channel = Channel(Tuple(Bytes, Channel(Nil))).new(128)
      spawn_writer_loop
    end

    private def spawn_writer_loop
      spawn do
        loop do
          break if @socket.closed?
          begin
            bytes, done_channel = @write_channel.receive
            size = bytes.size.to_i32
            @socket.write_bytes(size, IO::ByteFormat::BigEndian)
            @socket.write(bytes)
            @socket.flush
            done_channel.send(nil)
          rescue ex : Channel::ClosedError | IO::Error
            break
          rescue ex
            # log or handle other errors
            break
          end
        end
      end
    end

    def send_request(bytes : Bytes)
      done_channel = Channel(Nil).new(1)
      @write_channel.send({bytes, done_channel})
      done_channel.receive
    end

    def read_response : IO::Memory
      # Reads message size (4-byte Int32)
      size = @socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
      raise "Invalid response size: #{size}" if size <= 0

      buf = Bytes.new(size)
      @socket.read_fully(buf)
      IO::Memory.new(buf)
    end

    def closed? : Bool
      @socket.closed?
    end

    def close
      @socket.close
      @write_channel.close
    end
  end
end
