require "socket"
require "openssl"

module Kafkaesque
  class Connection
    getter host : String
    getter port : Int32
    getter use_ssl : Bool
    @socket : TCPSocket | OpenSSL::SSL::Socket::Client
    @mutex = Mutex.new

    # KIP-227 incremental fetch session state for Client#fetch_many, which
    # multiplexes all of a topic's assigned partitions routed to this
    # connection into a single FetchRequest. Safe to mutate without an
    # additional lock: #send_request/#read_response already bracket a full
    # request-response cycle under @mutex, and this state is only ever
    # touched from within that window. Scoped to a single topic at a time —
    # fine for the group-managed consumer flow (KIP-848/classic fallback),
    # which only ever tracks one active subscribed topic; a session on a
    # different topic resets it (see Client#fetch_many).
    property fetch_session_id : Int32 = 0
    property fetch_session_epoch : Int32 = -1
    property fetch_session_topic : String? = nil
    property fetch_session_partitions : Set(Int32) = Set(Int32).new

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
