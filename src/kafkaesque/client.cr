require "json"
require "./client/*"

module Kafkaesque
  # Pending batch entry: records queued for a topic+partition, waiting to be flushed
  private record BatchEntry, topic : String, partition : Int32, records : Array(Protocol::Record)

  class Client
    property client_id : String
    property session_timeout_ms : Int32 = 10000
    property heartbeat_interval_ms : Int32 = 3000
    property max_poll_interval_ms : Int32 = 300000
    property userid : String? = nil
    property on_deliver : (String, Int32, Int64, Exception? -> Void)? = nil

    # Idempotent producer state
    getter producer_id : Int64 = -1_i64
    getter producer_epoch : Int16 = -1_i16
    @sequence_numbers : Hash(String, Int32) = {} of String => Int32 # "topic:partition" => next_seq

    property batch_linger_ms : Int32 = 5
    property batch_max_size : Int32 = 100
    property acks : Int16 = 1_i16
    property compression : Int16 = 0_i16
    @batch_mutex : Mutex = Mutex.new
    @pending_batch : Hash(Tuple(String, Int32), Array(Protocol::Record)) = {} of Tuple(String, Int32) => Array(Protocol::Record)
    @batch_channel : Channel(Nil) = Channel(Nil).new(1)
    @batch_fiber_running : Bool = false

    # Consumer prefetch queue
    @prefetch_channel : Channel(Protocol::Record) = Channel(Protocol::Record).new(500)
    @prefetch_fiber_running : Bool = false

    @connection : Connection?
    @correlation_id : Int32 = 0
    @heartbeat_fiber_running = false
    property oauth_token_provider : (-> String)? = nil
    @broker_connections = {} of Int32 => Connection
    @partition_leaders = {} of String => Int32
    @brokers = {} of Int32 => Protocol::Broker
    @stats_callbacks = [] of (String -> Void)
    getter produced_messages_count : Int64 = 0_i64
    getter produced_bytes_count : Int64 = 0_i64
    getter consumed_messages_count : Int64 = 0_i64

    def initialize(
      @host : String,
      @port : Int32,
      @use_ssl : Bool = false,
      @sasl_token : String? = nil,
      @client_id = "kafkaesque-crystal",
      @ssl_context : OpenSSL::SSL::Context::Client? = nil,
      @oauth_token_provider : (-> String)? = nil,
    )
    end

    def connect
      conn = Connection.new(@host, @port, @use_ssl, @ssl_context)
      @connection = conn

      token = @oauth_token_provider.try(&.call) || @sasl_token
      if token
        authenticate_sasl(conn, token)
      end
    end

    private def authenticate_sasl(conn : Connection, token : String)
      # 1. SASL Handshake
      handshake_req = Protocol::SaslHandshakeRequest.new("OAUTHBEARER")

      handshake_io = IO::Memory.new
      handshake_enc = Protocol::Encoder.new(handshake_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::SaslHandshakeRequest::API_KEY,
        api_version: Protocol::SaslHandshakeRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id
      )

      req_header.serialize(handshake_enc)
      handshake_req.serialize(handshake_enc)

      conn.send_request(handshake_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
      handshake_resp = Protocol::SaslHandshakeResponse.deserialize(response_dec)

      if handshake_resp.error_code != 0
        raise "SASL Handshake failed with error code: #{handshake_resp.error_code}"
      end

      # 2. SASL Authenticate
      payload = Protocol::SaslAuthenticateRequest.oauthbearer_payload(token, @host, @port)
      auth_req = Protocol::SaslAuthenticateRequest.new(payload)

      auth_io = IO::Memory.new
      auth_enc = Protocol::Encoder.new(auth_io)

      auth_header = Protocol::RequestHeader.new(
        api_key: Protocol::SaslAuthenticateRequest::API_KEY,
        api_version: Protocol::SaslAuthenticateRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id
      )

      auth_header.serialize(auth_enc)
      auth_req.serialize(auth_enc)

      conn.send_request(auth_io.to_slice)

      auth_response_io = conn.read_response
      auth_response_dec = Protocol::Decoder.new(auth_response_io)

      Protocol::ResponseHeader.deserialize(auth_response_dec, flexible: false)
      auth_resp = Protocol::SaslAuthenticateResponse.deserialize(auth_response_dec)

      if auth_resp.error_code != 0
        raise "SASL Authentication failed: #{auth_resp.error_message} (code: #{auth_resp.error_code})"
      end
    end

    def fetch_metadata(topics : Array(String)? = nil) : Protocol::MetadataResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      req = Protocol::MetadataRequest.new(topics)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::MetadataRequest::API_KEY,
        api_version: Protocol::MetadataRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
      Protocol::MetadataResponse.deserialize(response_dec)
    end

    # -----------------------------------------------------------------------
    # Idempotent Producer: initializes producer ID and epoch from the broker
    # Must be called before using idempotent_produce or flush_batch
    # -----------------------------------------------------------------------
    def init_producer_id(transactional_id : String? = nil, transaction_timeout_ms : Int32 = 10000) : Protocol::InitProducerIdResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      req = Protocol::InitProducerIdRequest.new(transactional_id, transaction_timeout_ms)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::InitProducerIdRequest::API_KEY,
        api_version: Protocol::InitProducerIdRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: false
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
      resp = Protocol::InitProducerIdResponse.deserialize(response_dec)

      if resp.error_code != 0
        raise "InitProducerId failed with error code: #{resp.error_code}"
      end

      @producer_id = resp.producer_id
      @producer_epoch = resp.producer_epoch
      @sequence_numbers.clear

      resp
    end

    # Returns true if idempotent mode is active (producer_id has been obtained)
    def idempotent? : Bool
      @producer_id >= 0
    end

    def connection_for_partition(topic : String, partition : Int32) : Connection
      slot = "#{topic}:#{partition}"
      node_id = @partition_leaders[slot]? || refresh_partition_metadata(topic, slot)

      if node_id && (broker = @brokers[node_id]?)
        get_or_establish_broker_connection(node_id, broker)
      else
        @connection || raise "Client is not connected. Call #connect first."
      end
    end

    private def refresh_partition_metadata(topic : String, slot : String) : Int32?
      begin
        meta = fetch_metadata([topic])
        meta.brokers.each do |b|
          @brokers[b.node_id] = b
        end
        meta.topics.each do |t|
          t.partitions.each do |p|
            @partition_leaders["#{t.name}:#{p.partition_index}"] = p.leader_id
          end
        end
      rescue ex
        # fallback
      end
      @partition_leaders[slot]?
    end

    private def get_or_establish_broker_connection(node_id : Int32, broker : Protocol::Broker) : Connection
      conn = @broker_connections[node_id]?
      if conn.nil? || conn.closed?
        conn = Connection.new(broker.host, broker.port, @use_ssl, @ssl_context)
        @broker_connections[node_id] = conn
        if token = @oauth_token_provider.try(&.call) || @sasl_token
          authenticate_sasl(conn, token)
        end
      end
      conn
    end

    def on_stats(&block : String -> Void)
      @stats_callbacks << block
    end

    def emit_stats
      return if @stats_callbacks.empty?

      stats_json = {
        "client_id"          => @client_id,
        "produced_messages"  => @produced_messages_count,
        "produced_bytes"     => @produced_bytes_count,
        "consumed_messages"  => @consumed_messages_count,
        "broker_connections" => @broker_connections.size,
        "active_coordinator" => @connection.nil? ? false : true,
      }.to_json

      @stats_callbacks.each &.call(stats_json)
    end

    def self.connect_first(
      servers : Array(String),
      sasl_token : String? = nil,
      client_id : String = "kafkaesque-crystal",
      oauth_token_provider : (-> String)? = nil,
      use_ssl : Bool = false,
      ssl_context : OpenSSL::SSL::Context::Client? = nil,
    ) : Client
      last_err = nil
      servers.each do |server|
        parts = server.split(":")
        host = parts[0]
        port = parts.size > 1 ? parts[1].to_i : 9092
        begin
          client = Client.new(
            host: host,
            port: port,
            use_ssl: use_ssl,
            sasl_token: sasl_token,
            client_id: client_id,
            ssl_context: ssl_context,
            oauth_token_provider: oauth_token_provider
          )
          client.connect
          return client
        rescue ex
          last_err = ex
          Log.debug { "Failed to connect to bootstrap server #{server}: #{ex.message}" }
        end
      end
      raise last_err || Exception.new("No bootstrap servers configured")
    end

    def close
      stop_heartbeat_fiber
      stop_batch_fiber
      stop_prefetch
      @broker_connections.each_value do |conn|
        begin
          conn.close
        rescue
        end
      end
      @broker_connections.clear
      @connection.try(&.close)
      @connection = nil
    end

    private def next_correlation_id : Int32
      @correlation_id += 1
    end
  end
end
