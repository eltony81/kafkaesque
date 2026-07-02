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
    @partition_leader_epochs = {} of String => Int32
    @partition_replicas = {} of String => Array(Int32)
    @brokers = {} of Int32 => Protocol::Broker
    @topic_ids = {} of String => Bytes
    # Guards @broker_connections/@partition_leaders/@partition_leader_epochs/
    # @partition_replicas/@brokers, which are mutated concurrently by
    # per-partition fetcher fibers (and, under -Dpreview_mt, potentially
    # different OS threads at once).
    @metadata_mutex = Mutex.new
    @stats_callbacks = [] of (String -> Void)
    getter produced_messages_count : Int64 = 0_i64
    getter produced_bytes_count : Int64 = 0_i64
    getter consumed_messages_count : Int64 = 0_i64
    property settings : Hash(String, String)?

    property buffer_memory : Int64 = 33554432_i64
    property max_block_ms : Int32 = 60000
    property fetch_max_bytes : Int32 = 1048576
    property max_partition_fetch_bytes : Int32 = 1048576
    property metadata_refresh_interval_ms : Int32 = 300000
    property oauth_refresh_interval_ms : Int32 = 300000
    property buffer_memory_used : Int64 = 0_i64
    @metadata_refresh_running : Bool = false
    @oauth_refresh_running : Bool = false
    @current_oauth_token : String? = nil
    property metrics_port : Int32? = nil
    @metrics_server : MetricsServer? = nil
    property ssl_reload_interval_ms : Int32 = 0
    @ssl_reload_running = false
    @cert_last_mtime : Time? = nil
    @key_last_mtime : Time? = nil

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

      if settings = @settings
        if val = settings["buffer.memory"]?
          @buffer_memory = val.to_i64
        end
        if val = settings["max.block.ms"]?
          @max_block_ms = val.to_i
        end
        if val = settings["fetch.max.bytes"]?
          @fetch_max_bytes = val.to_i
        end
        if val = settings["max.partition.fetch.bytes"]?
          @max_partition_fetch_bytes = val.to_i
        end
        if val = settings["topic.metadata.refresh.interval.ms"]?
          @metadata_refresh_interval_ms = val.to_i
        end
        if val = settings["sasl.oauthbearer.token.refresh.interval.ms"]?
          @oauth_refresh_interval_ms = val.to_i
        end
        if val = settings["linger.ms"]?
          @batch_linger_ms = val.to_i
        end
        if val = settings["batch.num.messages"]?
          @batch_max_size = val.to_i
        end
        if val = settings["metrics.prometheus.port"]?
          @metrics_port = val.to_i
        end
        if val = settings["ssl.keystore.reload.interval.ms"]?
          @ssl_reload_interval_ms = val.to_i
        end
      end
    end

    alias SASLAuthenticatorBuilder = (Connection, Hash(String, String) -> Void)
    @@custom_sasl_mechanisms = {} of String => SASLAuthenticatorBuilder

    def self.register_sasl_mechanism(name : String, &builder : SASLAuthenticatorBuilder)
      @@custom_sasl_mechanisms[name.upcase] = builder
    end

    def prometheus_metrics : String
      String.build do |str|
        str << "# HELP kafkaesque_produced_messages_total Total number of produced messages\n"
        str << "# TYPE kafkaesque_produced_messages_total counter\n"
        str << "kafkaesque_produced_messages_total{client_id=\"#{@client_id}\"} #{@produced_messages_count}\n\n"

        str << "# HELP kafkaesque_produced_bytes_total Total size of produced messages in bytes\n"
        str << "# TYPE kafkaesque_produced_bytes_total counter\n"
        str << "kafkaesque_produced_bytes_total{client_id=\"#{@client_id}\"} #{@produced_bytes_count}\n\n"

        str << "# HELP kafkaesque_consumed_messages_total Total number of consumed messages\n"
        str << "# TYPE kafkaesque_consumed_messages_total counter\n"
        str << "kafkaesque_consumed_messages_total{client_id=\"#{@client_id}\"} #{@consumed_messages_count}\n\n"

        str << "# HELP kafkaesque_broker_connections Total number of active broker connections\n"
        str << "# TYPE kafkaesque_broker_connections gauge\n"
        str << "kafkaesque_broker_connections{client_id=\"#{@client_id}\"} #{@metadata_mutex.synchronize { @broker_connections.size }}\n"
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

      # Peer verification overrides
      verify_peer = true
      if settings["ssl.endpoint.identification.algorithm"]? == "none" || settings["ssl.verify.peer"]? == "false"
        verify_peer = false
      end
      ctx.verify_mode = verify_peer ? OpenSSL::SSL::VerifyMode::PEER : OpenSSL::SSL::VerifyMode::NONE
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

      if builder = @@custom_sasl_mechanisms[mechanism]?
        builder.call(conn, @settings || {} of String => String)
      elsif mechanism == "OAUTHBEARER"
        token = @oauth_token_provider.try(&.call) || @sasl_token || @settings.try(&.[]?("sasl.password")) || @settings.try(&.[]?("sasl.oauthbearer.token"))
        if token
          @current_oauth_token = token
          authenticate_sasl(conn, token)
        end
        start_oauth_refresh_fiber if @oauth_token_provider
      elsif mechanism == "SCRAM-SHA-256" || mechanism == "SCRAM-SHA-512"
        username = @settings.try(&.[]?("sasl.scram.username")) || @settings.try(&.[]?("sasl.username")) || ""
        password = @settings.try(&.[]?("sasl.scram.password")) || @settings.try(&.[]?("sasl.password")) || ""
        authenticate_scram(conn, username, password, mechanism)
      end

      start_metadata_refresh_fiber if @metadata_refresh_interval_ms > 0
      start_ssl_reload_fiber if @ssl_reload_interval_ms > 0
      start_metrics_server if @metrics_port
    end

    private def authenticate_scram(conn : Connection, username : String, password : String, mechanism : String)
      algo = case mechanism
             when "SCRAM-SHA-256"
               :sha256
             when "SCRAM-SHA-512"
               :sha512
             else
               :sha1
             end
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
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)

      Protocol::ResponseHeader.deserialize(response_dec, flexible: true)
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
      node_id = @metadata_mutex.synchronize { @partition_leaders[slot]? } || refresh_partition_metadata(topic, slot)
      broker = node_id ? @metadata_mutex.synchronize { @brokers[node_id]? } : nil

      if node_id && broker
        get_or_establish_broker_connection(node_id, broker)
      else
        @connection || raise "Client is not connected. Call #connect first."
      end
    end

    private def refresh_partition_metadata(topic : String, slot : String) : Int32?
      begin
        meta = fetch_metadata([topic])
        @metadata_mutex.synchronize do
          meta.brokers.each do |b|
            @brokers[b.node_id] = b
          end
          meta.topics.each do |t|
            @topic_ids[t.name] = t.topic_id
            t.partitions.each do |p|
              @partition_leaders["#{t.name}:#{p.partition_index}"] = p.leader_id
              @partition_leader_epochs["#{t.name}:#{p.partition_index}"] = p.leader_epoch
              @partition_replicas["#{t.name}:#{p.partition_index}"] = p.replica_nodes
            end
          end
        end
      rescue ex
        Log.debug { "Partition metadata refresh for #{topic} failed: #{ex.message}" }
      end
      @metadata_mutex.synchronize { @partition_leaders[slot]? }
    end

    # Topic UUID as returned by the last Metadata refresh (Metadata v10+, KIP-516),
    # used to build KIP-932 FindCoordinator SHARE keys ("groupId:topicId:partition").
    def topic_id_for(topic : String) : Bytes?
      @metadata_mutex.synchronize { @topic_ids[topic]? }
    end

    # Leader epoch as of the last Metadata refresh (KIP-320) — used both to
    # fence Fetch requests against a stale/former leader and, after a leader
    # change, to detect log truncation via OffsetForLeaderEpoch.
    def leader_epoch_for(topic : String, partition : Int32) : Int32?
      @metadata_mutex.synchronize { @partition_leader_epochs["#{topic}:#{partition}"]? }
    end

    def partitions_count(topic : String) : Int32
      count = @metadata_mutex.synchronize { @partition_leaders.keys.count { |k| k.starts_with?("#{topic}:") } }
      if count == 0
        refresh_partition_metadata(topic, "#{topic}:0")
        count = @metadata_mutex.synchronize { @partition_leaders.keys.count { |k| k.starts_with?("#{topic}:") } }
      end
      count > 0 ? count : 1
    end

    private def authenticate_connection(conn : Connection)
      mechanism = @settings.try(&.[]?("sasl.mechanism")).try(&.upcase) || "OAUTHBEARER"

      if builder = @@custom_sasl_mechanisms[mechanism]?
        builder.call(conn, @settings || {} of String => String)
      elsif mechanism == "OAUTHBEARER"
        token = @current_oauth_token || @oauth_token_provider.try(&.call) || @sasl_token || @settings.try(&.[]?("sasl.password")) || @settings.try(&.[]?("sasl.oauthbearer.token"))
        if token
          @current_oauth_token = token
          authenticate_sasl(conn, token)
        end
      elsif mechanism == "SCRAM-SHA-256" || mechanism == "SCRAM-SHA-512" || mechanism == "SCRAM-SHA-1"
        username = @settings.try(&.[]?("sasl.scram.username")) || @settings.try(&.[]?("sasl.username")) || ""
        password = @settings.try(&.[]?("sasl.scram.password")) || @settings.try(&.[]?("sasl.password")) || ""
        authenticate_scram(conn, username, password, mechanism)
      end
    end

    private def get_or_establish_broker_connection(node_id : Int32, broker : Protocol::Broker) : Connection
      existing = @metadata_mutex.synchronize { @broker_connections[node_id]? }
      return existing if existing && !existing.closed?

      # Connect/retry (network I/O + backoff sleeps) happen outside the lock so
      # a slow/failing connection to one broker doesn't stall unrelated fibers
      # (other partitions' fetchers, the metadata-refresh loop, stats, close)
      # that just need the mutex briefly to touch these hashes.
      backoff = Backoff.new(base: 100.0, max: 10000.0)
      attempts = 0
      conn = nil
      loop do
        begin
          host = broker.host == "localhost" ? "127.0.0.1" : broker.host
          conn = Connection.new(host, broker.port, @use_ssl, @ssl_context)
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
      new_conn = conn.not_nil!

      @metadata_mutex.synchronize do
        # Another fiber may have raced us to establish this connection first;
        # keep whichever is already stored and discard our redundant one.
        if (current = @broker_connections[node_id]?) && !current.closed?
          new_conn.close rescue nil
          current
        else
          @broker_connections[node_id] = new_conn
          new_conn
        end
      end
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
        "broker_connections" => @metadata_mutex.synchronize { @broker_connections.size },
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
      stop_metadata_refresh_fiber
      stop_oauth_refresh_fiber
      stop_ssl_reload_fiber
      stop_metrics_server
      conns = @metadata_mutex.synchronize do
        snapshot = @broker_connections.values
        @broker_connections.clear
        snapshot
      end
      conns.each do |conn|
        begin
          conn.close
        rescue
        end
      end
      @connection.try(&.close)
      @connection = nil
    end

    private def start_metadata_refresh_fiber
      return if @metadata_refresh_running
      @metadata_refresh_running = true
      spawn do
        while @metadata_refresh_running
          sleep @metadata_refresh_interval_ms.milliseconds
          break unless @metadata_refresh_running
          begin
            topics = @metadata_mutex.synchronize { @partition_leaders.keys.map { |k| k.split(":")[0] }.uniq }
            fetch_metadata(topics.empty? ? nil : topics)
          rescue ex
            Log.debug { "Background metadata refresh failed: #{ex.message}" }
          end
        end
      end
    end

    private def stop_metadata_refresh_fiber
      @metadata_refresh_running = false
    end

    private def start_oauth_refresh_fiber
      return if @oauth_refresh_running
      @oauth_refresh_running = true
      spawn do
        while @oauth_refresh_running
          sleep @oauth_refresh_interval_ms.milliseconds
          break unless @oauth_refresh_running
          if provider = @oauth_token_provider
            begin
              @current_oauth_token = provider.call
            rescue ex
              Log.warn { "Background OAuth token refresh failed: #{ex.message}" }
            end
          end
        end
      end
    end

    private def stop_oauth_refresh_fiber
      @oauth_refresh_running = false
    end

    private def start_ssl_reload_fiber
      return if @ssl_reload_running || @ssl_reload_interval_ms <= 0
      @ssl_reload_running = true

      cert_path = @settings.try(&.[]?("ssl.keystore.location"))
      key_path = @settings.try(&.[]?("ssl.keystore.key.location")) || cert_path

      return unless cert_path && File.exists?(cert_path)

      @cert_last_mtime = File.info(cert_path).modification_time
      @key_last_mtime = key_path && File.exists?(key_path) ? File.info(key_path).modification_time : nil

      spawn do
        while @ssl_reload_running
          sleep @ssl_reload_interval_ms.milliseconds
          break unless @ssl_reload_running

          begin
            changed = false
            if cert_path && File.exists?(cert_path)
              curr_mtime = File.info(cert_path).modification_time
              if curr_mtime != @cert_last_mtime
                @cert_last_mtime = curr_mtime
                changed = true
              end
            end
            if key_path && File.exists?(key_path)
              curr_mtime = File.info(key_path).modification_time
              if curr_mtime != @key_last_mtime
                @key_last_mtime = curr_mtime
                changed = true
              end
            end

            if changed && (settings = @settings)
              Log.info { "Dynamic SSL/TLS Keystore change detected. Rebuilding SSL/TLS Client Context..." }
              @ssl_context = Client.build_ssl_context(settings)
            end
          rescue ex
            Log.warn { "Failed to check or reload SSL context: #{ex.message}" }
          end
        end
      end
    end

    private def stop_ssl_reload_fiber
      @ssl_reload_running = false
    end

    private def start_metrics_server
      return if @metrics_server
      if port = @metrics_port
        srv = MetricsServer.new(port) { prometheus_metrics }
        srv.start
        @metrics_server = srv
      end
    end

    private def stop_metrics_server
      if srv = @metrics_server
        srv.close
        @metrics_server = nil
      end
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
        replicas = @metadata_mutex.synchronize { @partition_replicas[slot]? } || begin
          refresh_partition_metadata(topic, slot)
          @metadata_mutex.synchronize { @partition_replicas[slot]? }
        end

        if replicas
          replicas.each do |node_id|
            if broker = @metadata_mutex.synchronize { @brokers[node_id]? }
              if broker.rack == rack
                return get_or_establish_broker_connection(node_id, broker)
              end
            end
          end
        end
      end

      connection_for_partition(topic, partition)
    end

    # Fetches from all given partitions of a topic (identified by topic_id,
    # KIP-516/932), one ShareFetchRequest per partition leader (mirrors regular
    # Fetch partition-leader routing), merging the results into a single
    # response. share_session_epoch follows the KIP-227-style incremental
    # fetch session protocol: 0 opens/resets the session, -1 closes it,
    # otherwise the caller's running per-session counter. pending_acks lets
    # the caller piggyback acknowledgements for previously-fetched records
    # (keyed by partition) onto this same request.
    def share_fetch(
      group_id : String,
      member_id : String,
      topic : String,
      topic_id : Bytes,
      partitions : Array(Int32),
      share_session_epoch : Int32,
      pending_acks : Hash(Int32, Array(Protocol::ShareAcknowledgementBatch)) = {} of Int32 => Array(Protocol::ShareAcknowledgementBatch),
      max_bytes : Int32 = 1048576,
    ) : Protocol::ShareFetchResponse
      by_conn = Hash(Connection, Array(Int32)).new { |h, k| h[k] = [] of Int32 }
      partitions.each do |partition|
        conn = connection_for_partition(topic, partition)
        by_conn[conn] << partition
      end

      merged_partitions = [] of Protocol::ShareFetchResponsePartition
      error_code = 0_i16
      error_message = nil
      acquisition_lock_timeout_ms = 0

      by_conn.each do |conn, parts|
        p_reqs = parts.map { |p| Protocol::ShareFetchPartitionRequest.new(p, pending_acks[p]? || [] of Protocol::ShareAcknowledgementBatch) }
        t_req = Protocol::ShareFetchTopicRequest.new(topic_id, p_reqs)
        req = Protocol::ShareFetchRequest.new(group_id, member_id, share_session_epoch, [t_req], max_bytes: max_bytes)

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
        resp = Protocol::ShareFetchResponse.deserialize(response_dec)

        error_code = resp.error_code if resp.error_code != 0
        error_message ||= resp.error_message
        acquisition_lock_timeout_ms = resp.acquisition_lock_timeout_ms
        resp.topics.each { |t| merged_partitions.concat(t.partitions) }
      end

      Protocol::ShareFetchResponse.new(error_code, error_message, acquisition_lock_timeout_ms, [Protocol::ShareFetchResponseTopic.new(topic_id, merged_partitions)])
    end

    def share_acknowledge(
      group_id : String,
      member_id : String,
      topic : String,
      topic_id : Bytes,
      partition : Int32,
      share_session_epoch : Int32,
      batches : Array(Protocol::ShareAcknowledgementBatch),
    ) : Protocol::ShareAcknowledgeResponse
      conn = connection_for_partition(topic, partition)

      ack_part = Protocol::ShareAcknowledgePartitionRequest.new(partition, batches)
      ack_topic = Protocol::ShareAcknowledgeTopicRequest.new(topic_id, [ack_part])
      req = Protocol::ShareAcknowledgeRequest.new(group_id, member_id, share_session_epoch, [ack_topic])

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
