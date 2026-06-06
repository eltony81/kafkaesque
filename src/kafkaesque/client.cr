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
    property max_retries : Int32 = 3
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
    property client_rack : String? = nil
    getter api_versions : Array(Protocol::ApiVersionInfo) = [] of Protocol::ApiVersionInfo
    @broker_connections = {} of Int32 => Connection
    @partition_leaders = {} of String => Int32
    @partition_replicas = {} of String => Array(Int32)
    @brokers = {} of Int32 => Protocol::Broker
    @stats_callbacks = [] of (String -> Void)
    getter produced_messages_count : Int64 = 0_i64
    getter produced_bytes_count : Int64 = 0_i64
    getter consumed_messages_count : Int64 = 0_i64
    property settings : Hash(String, String)?

    def initialize(
      @host : String,
      @port : Int32,
      use_ssl : Bool = false,
      @sasl_token : String? = nil,
      @client_id = "kafkaesque-crystal",
      ssl_context : OpenSSL::SSL::Context::Client? = nil,
      @oauth_token_provider : (-> String)? = nil,
      @max_retries : Int32 = 3,
      @settings : Hash(String, String)? = nil,
    )
      @use_ssl = use_ssl || (settings.try(&.[]?("security.protocol")) == "SSL" || settings.try(&.[]?("security.protocol")) == "SASL_SSL")
      @ssl_context = ssl_context
      if @use_ssl && @ssl_context.nil? && settings
        @ssl_context = Client.build_ssl_context(settings)
      end
    end

    def self.build_ssl_context(settings : Hash(String, String)) : OpenSSL::SSL::Context::Client
      ctx = OpenSSL::SSL::Context::Client.new
      if ca_file = settings["ssl.truststore.location"]?
        ctx.ca_certificates = ca_file
      end

      if cert_file = settings["ssl.keystore.location"]?
        key_file = settings["ssl.keystore.key.location"]? || cert_file
        ctx.certificate_chain = cert_file
        ctx.private_key = key_file
      end
      ctx
    end

    def connect
      conn = Connection.new(@host, @port, @use_ssl, @ssl_context)
      @connection = conn

      begin
        query_api_versions
      rescue ex
        Log.debug { "ApiVersions query failed: #{ex.message}. Continuing connection..." }
      end

      mechanism = @settings.try(&.[]?("sasl.mechanism")).try(&.upcase) || "OAUTHBEARER"

      if mechanism == "OAUTHBEARER"
        token = @oauth_token_provider.try(&.call) || @sasl_token || @settings.try(&.[]?("sasl.password")) || @settings.try(&.[]?("sasl.oauthbearer.token"))
        if token
          authenticate_sasl(conn, token)
        end
      elsif mechanism == "SCRAM-SHA-256" || mechanism == "SCRAM-SHA-512"
        username = @settings.try(&.[]?("sasl.scram.username")) || @settings.try(&.[]?("sasl.username")) || ""
        password = @settings.try(&.[]?("sasl.scram.password")) || @settings.try(&.[]?("sasl.password")) || ""
        authenticate_scram(conn, username, password, mechanism)
      end
    end

    private def authenticate_scram(conn : Connection, username : String, password : String, mechanism : String)
      algo = mechanism == "SCRAM-SHA-256" ? :sha256 : :sha512
      authenticator = Protocol::ScramAuthenticator.new(username, password, algo)

      # 1. Send SaslHandshakeRequest specifying the mechanism
      handshake_req = Protocol::SaslHandshakeRequest.new(mechanism)
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
        raise "SASL Handshake failed for #{mechanism} with error code: #{handshake_resp.error_code}"
      end

      # 2. Client First Message -> SaslAuthenticateRequest
      client_first = authenticator.client_first_message
      auth_req_1 = Protocol::SaslAuthenticateRequest.new(client_first.to_slice)

      auth_io_1 = IO::Memory.new
      auth_enc_1 = Protocol::Encoder.new(auth_io_1)

      auth_header_1 = Protocol::RequestHeader.new(
        api_key: Protocol::SaslAuthenticateRequest::API_KEY,
        api_version: Protocol::SaslAuthenticateRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id
      )

      auth_header_1.serialize(auth_enc_1)
      auth_req_1.serialize(auth_enc_1)

      conn.send_request(auth_io_1.to_slice)

      auth_resp_io_1 = conn.read_response
      auth_resp_dec_1 = Protocol::Decoder.new(auth_resp_io_1)

      Protocol::ResponseHeader.deserialize(auth_resp_dec_1, flexible: false)
      auth_resp_1 = Protocol::SaslAuthenticateResponse.deserialize(auth_resp_dec_1)

      if auth_resp_1.error_code != 0
        raise "SCRAM Client First authenticate failed: #{auth_resp_1.error_message} (code: #{auth_resp_1.error_code})"
      end

      server_first_bytes = auth_resp_1.auth_bytes || raise "Server first message is empty"
      server_first = String.new(server_first_bytes)

      # 3. Client Final Message
      client_final, expected_server_signature = authenticator.process_server_first_message(server_first)

      auth_req_2 = Protocol::SaslAuthenticateRequest.new(client_final.to_slice)
      auth_io_2 = IO::Memory.new
      auth_enc_2 = Protocol::Encoder.new(auth_io_2)

      auth_header_2 = Protocol::RequestHeader.new(
        api_key: Protocol::SaslAuthenticateRequest::API_KEY,
        api_version: Protocol::SaslAuthenticateRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id
      )

      auth_header_2.serialize(auth_enc_2)
      auth_req_2.serialize(auth_enc_2)

      conn.send_request(auth_io_2.to_slice)

      auth_resp_io_2 = conn.read_response
      auth_resp_dec_2 = Protocol::Decoder.new(auth_resp_io_2)

      Protocol::ResponseHeader.deserialize(auth_resp_dec_2, flexible: false)
      auth_resp_2 = Protocol::SaslAuthenticateResponse.deserialize(auth_resp_dec_2)

      if auth_resp_2.error_code != 0
        raise "SCRAM Client Final authenticate failed: #{auth_resp_2.error_message} (code: #{auth_resp_2.error_code})"
      end

      server_final_bytes = auth_resp_2.auth_bytes || raise "Server final message is empty"
      server_final = String.new(server_final_bytes)

      # 4. Verify Server Signature
      unless authenticator.verify_server_final_message(server_final, expected_server_signature)
        raise "Server signature verification failed in SCRAM authentication"
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
            @partition_replicas["#{t.name}:#{p.partition_index}"] = p.replica_nodes
          end
        end
      rescue ex
        # fallback
      end
      @partition_leaders[slot]?
    end

    private def authenticate_connection(conn : Connection)
      mechanism = @settings.try(&.[]?("sasl.mechanism")).try(&.upcase) || "OAUTHBEARER"

      if mechanism == "OAUTHBEARER"
        token = @oauth_token_provider.try(&.call) || @sasl_token || @settings.try(&.[]?("sasl.password")) || @settings.try(&.[]?("sasl.oauthbearer.token"))
        if token
          authenticate_sasl(conn, token)
        end
      elsif mechanism == "SCRAM-SHA-256" || mechanism == "SCRAM-SHA-512"
        username = @settings.try(&.[]?("sasl.scram.username")) || @settings.try(&.[]?("sasl.username")) || ""
        password = @settings.try(&.[]?("sasl.scram.password")) || @settings.try(&.[]?("sasl.password")) || ""
        authenticate_scram(conn, username, password, mechanism)
      end
    end

    private def get_or_establish_broker_connection(node_id : Int32, broker : Protocol::Broker) : Connection
      conn = @broker_connections[node_id]?
      if conn.nil? || conn.closed?
        backoff = Backoff.new(base: 100.0, max: 10000.0)
        attempts = 0
        loop do
          begin
            conn = Connection.new(broker.host, broker.port, @use_ssl, @ssl_context)
            @broker_connections[node_id] = conn
            authenticate_connection(conn)
            break
          rescue ex
            attempts += 1
            if attempts > @max_retries
              raise ex
            end
            sleep_ms = backoff.compute(attempts)
            Log.debug { "Failed to establish connection to broker #{node_id} (#{broker.host}:#{broker.port}). Retrying in #{sleep_ms.round(2)}ms..." }
            sleep sleep_ms.milliseconds
          end
        end
      end
      conn.not_nil!
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
      max_retries : Int32 = 3,
      settings : Hash(String, String)? = nil,
    ) : Client
      last_err = nil
      backoff = Backoff.new(base: 100.0, max: 10000.0)

      (max_retries + 1).times do |attempt|
        servers.each do |server|
          parts = server.split(":")
          host = parts[0]
          port = parts.size > 1 ? parts[1].to_i : 9092

          actual_ssl = use_ssl || (settings.try(&.[]?("security.protocol")) == "SSL" || settings.try(&.[]?("security.protocol")) == "SASL_SSL")
          actual_ctx = ssl_context
          if actual_ssl && actual_ctx.nil? && settings
            actual_ctx = build_ssl_context(settings)
          end

          begin
            client = Client.new(
              host: host,
              port: port,
              use_ssl: actual_ssl,
              sasl_token: sasl_token,
              client_id: client_id,
              ssl_context: actual_ctx,
              oauth_token_provider: oauth_token_provider,
              max_retries: max_retries,
              settings: settings
            )
            client.connect
            return client
          rescue ex
            last_err = ex
            Log.debug { "Failed to connect to bootstrap server #{server}: #{ex.message}" }
          end
        end

        if attempt < max_retries
          sleep_ms = backoff.compute(attempt)
          Log.debug { "All connection attempts failed. Retrying in #{sleep_ms.round(2)}ms..." }
          sleep sleep_ms.milliseconds
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

    def query_api_versions : Protocol::ApiVersionsResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      req = Protocol::ApiVersionsRequest.new("kafkaesque", Kafkaesque::VERSION)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::ApiVersionsRequest::API_KEY,
        api_version: Protocol::ApiVersionsRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
      resp = Protocol::ApiVersionsResponse.deserialize(response_dec)

      if resp.error_code != 0
        raise "ApiVersions failed with error code: #{resp.error_code}"
      end

      @api_versions = resp.api_keys
      resp
    end

    def closest_replica_connection_for_partition(topic : String, partition : Int32) : Connection
      slot = "#{topic}:#{partition}"
      if rack = @client_rack
        replicas = @partition_replicas[slot]? || begin
          refresh_partition_metadata(topic, slot)
          @partition_replicas[slot]?
        end

        if replicas
          replicas.each do |node_id|
            if broker = @brokers[node_id]?
              if broker.rack == rack
                return get_or_establish_broker_connection(node_id, broker)
              end
            end
          end
        end
      end

      connection_for_partition(topic, partition)
    end

    def share_fetch(group_id : String, member_id : String, topic : String, partition : Int32, max_bytes : Int32 = 1048576) : Protocol::ShareFetchResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      p_req = Protocol::ShareFetchPartition.new(partition, max_bytes)
      t_req = Protocol::ShareFetchTopic.new(topic, [p_req])
      req = Protocol::ShareFetchRequest.new(group_id, member_id, [t_req], max_bytes)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::ShareFetchRequest::API_KEY,
        api_version: Protocol::ShareFetchRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: true)
      Protocol::ShareFetchResponse.deserialize(response_dec)
    end

    def share_acknowledge(group_id : String, member_id : String, topic : String, partition : Int32, first_offset : Int64, last_offset : Int64, ack_type : Int8) : Protocol::ShareAcknowledgeResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      ack_info = Protocol::ShareAckInfo.new(first_offset, last_offset, ack_type)
      ack_part = Protocol::ShareAckPartition.new(partition, [ack_info])
      ack_topic = Protocol::ShareAckTopic.new(topic, [ack_part])
      req = Protocol::ShareAcknowledgeRequest.new(group_id, member_id, [ack_topic])

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::ShareAcknowledgeRequest::API_KEY,
        api_version: Protocol::ShareAcknowledgeRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: true)
      Protocol::ShareAcknowledgeResponse.deserialize(response_dec)
    end

    def get_telemetry_subscription(client_instance_id : Bytes = Bytes.new(16)) : Protocol::GetTelemetrySubscriptionsResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      req = Protocol::GetTelemetrySubscriptionsRequest.new(client_instance_id)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::GetTelemetrySubscriptionsRequest::API_KEY,
        api_version: Protocol::GetTelemetrySubscriptionsRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: true)
      Protocol::GetTelemetrySubscriptionsResponse.deserialize(response_dec)
    end

    def push_client_telemetry(subscription_id : Int32, client_instance_id : Bytes, metrics_data : Bytes, terminating : Bool = false, compression_type : Int8 = 0) : Protocol::PushTelemetryResponse
      conn = @connection || raise "Client is not connected. Call #connect first."

      req = Protocol::PushTelemetryRequest.new(
        client_instance_id: client_instance_id,
        subscription_id: subscription_id,
        terminating: terminating,
        compression_type: compression_type,
        metrics: metrics_data
      )

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::PushTelemetryRequest::API_KEY,
        api_version: Protocol::PushTelemetryRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: true)
      Protocol::PushTelemetryResponse.deserialize(response_dec)
    end

    private def next_correlation_id : Int32
      @correlation_id += 1
    end
  end
end
