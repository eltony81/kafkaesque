require "socket"
require "openssl"

module Kafkaesque
  class Connection
    getter host : String
    getter port : Int32
    getter use_ssl : Bool
    @socket : TCPSocket | OpenSSL::SSL::Socket::Client
    @mutex = Mutex.new

    def initialize(@host : String, @port : Int32, @use_ssl : Bool = false, context : OpenSSL::SSL::Context::Client? = nil)
      tcp = TCPSocket.new(@host, @port)
      tcp.tcp_nodelay = true
      tcp.read_timeout = 30.seconds
      tcp.write_timeout = 30.seconds
      if @use_ssl
        ctx = context || OpenSSL::SSL::Context::Client.new
        @socket = OpenSSL::SSL::Socket::Client.new(tcp, ctx)
      else
        @socket = tcp
      end
    end

    def send_request(bytes : Bytes)
      @mutex.lock
      size = bytes.size.to_i32
      @socket.write_bytes(size, IO::ByteFormat::BigEndian)
      @socket.write(bytes)
      @socket.flush
    rescue ex
      @mutex.unlock rescue nil
      raise ex
    end

    def read_response : IO::Memory
      size = @socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
      raise "Invalid response size: #{size}" if size <= 0

      buf = Bytes.new(size)
      @socket.read_fully(buf)
      IO::Memory.new(buf)
    ensure
      @mutex.unlock rescue nil
    end

    def closed? : Bool
      @socket.closed?
    end

    def close
      @socket.close
    end
  end
end
