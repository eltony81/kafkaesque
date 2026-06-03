module Kafkaesque
  class Producer
    class Config
      property bootstrap_servers : Array(String)
      property settings : Hash(String, String)
      property oauth_token_provider : (-> String)? = nil
      property compression_type : String? = nil

      def initialize(bootstrap_servers : Array(String) = ["localhost:9092"], compression_type : String? = nil, settings = {} of String => String)
        @bootstrap_servers = bootstrap_servers
        @compression_type = compression_type
        @settings = settings
        setup_oauth_provider
      end

      def self.build(&block : Config ->)
        cfg = new(bootstrap_servers: [] of String)
        block.call(cfg)
        cfg
      end

      def linger_ms=(val : Int32)
        set("linger.ms", val.to_s)
      end

      def linger_ms : Int32
        @settings["linger.ms"]?.try(&.to_i) || 0
      end

      def batch_num_messages=(val : Int32)
        set("batch.num.messages", val.to_s)
      end

      def batch_num_messages : Int32
        @settings["batch.num.messages"]?.try(&.to_i) || 1000
      end

      def idempotence=(val : Bool)
        set("enable.idempotence", val.to_s)
      end

      def idempotence : Bool
        @settings["enable.idempotence"]? == "true"
      end

      def acks=(val : String)
        set("acks", val)
      end

      def acks : String
        @settings["acks"]? || "1"
      end

      def set(key : String, value : String)
        @settings[key] = value
        setup_oauth_provider if key.starts_with?("sasl.oauthbearer")
      end

      private def setup_oauth_provider
        if endpoint = @settings["sasl.oauthbearer.token.endpoint.url"]?
          client_id = @settings["sasl.oauthbearer.client.id"]? || ""
          client_secret = @settings["sasl.oauthbearer.client.secret"]? || ""

          @oauth_token_provider = -> {
            response = HTTP::Client.post(
              endpoint,
              headers: HTTP::Headers{"Content-Type" => "application/x-www-form-urlencoded"},
              body: "grant_type=client_credentials&client_id=#{URI.encode_www_form(client_id)}&client_secret=#{URI.encode_www_form(client_secret)}"
            )
            if response.status_code != 200
              raise "Failed to fetch OAuth token from #{endpoint} (Status: #{response.status_code}): #{response.body}"
            end
            JSON.parse(response.body)["access_token"].as_s
          }
        end
      end
    end

    getter config : Config
    @client : Client?
    @transactional_id : String?
    @in_transaction : Bool = false
    @txn_partitions : Set(String) = Set(String).new

    def self.new(&block : Config ->)
      cfg = Config.build(&block)
      new(cfg)
    end

    def initialize(@config : Config)
      if @config.bootstrap_servers.empty?
        raise "No bootstrap servers configured"
      end

      sasl_token = @config.settings["sasl.token"]? || @config.settings["sasl.password"]?

      max_retries = (@config.settings["retries"]? || @config.settings["max_retries"]?).try(&.to_i) || 3

      # Create our main Client using connect_first
      client = Client.connect_first(
        servers: @config.bootstrap_servers,
        sasl_token: sasl_token,
        client_id: @config.settings["client.id"]? || "kafkaesque-producer",
        oauth_token_provider: @config.oauth_token_provider,
        max_retries: max_retries
      )

      # Configure batch accumulator options from settings
      if linger_ms = @config.settings["linger.ms"]?
        client.batch_linger_ms = linger_ms.to_i
      end
      if batch_size = @config.settings["batch.num.messages"]?
        client.batch_max_size = batch_size.to_i
      end

      # Map acks parameter ("all" -> -1, "1" -> 1, "0" -> 0)
      if acks_setting = @config.settings["acks"]?
        client.acks = case acks_setting
                      when "all" then -1_i16
                      when "0"   then 0_i16
                      else            1_i16
                      end
      end

      @transactional_id = @config.settings["transactional.id"]?
      @in_transaction = false
      @txn_partitions = Set(String).new

      # Configure compression setting (gzip -> 1_i16, snappy -> 2_i16, lz4 -> 3_i16, zstd -> 4_i16)
      if codec_setting = @config.compression_type || @config.settings["compression.type"]?
        client.compression = case codec_setting
                             when "gzip"   then 1_i16
                             when "snappy" then 2_i16
                             when "lz4"    then 3_i16
                             when "zstd"   then 4_i16
                             else               0_i16
                             end
      end

      @client = client

      # If transactional or idempotence is enabled, initialize the producer transaction / epoch ID
      if tx_id = @transactional_id
        client.init_producer_id(tx_id)
      elsif @config.settings["enable.idempotence"]? == "true"
        client.init_producer_id
      end
    end

    def begin_transaction
      tx_id = @transactional_id || raise "Producer is not configured for transactions. Set 'transactional.id' in settings."
      if @in_transaction
        raise "Transaction already in progress"
      end

      client = @client || raise "Producer is closed"
      if client.producer_id < 0
        client.init_producer_id(tx_id)
      end

      @in_transaction = true
      @txn_partitions.clear
    end

    def commit_transaction
      tx_id = @transactional_id || raise "Not in transactional mode"
      unless @in_transaction
        raise "No transaction in progress"
      end

      client = @client || raise "Producer is closed"
      client.flush_batch

      resp = client.end_txn(tx_id, client.producer_id, client.producer_epoch, true)
      if resp.error_code != 0
        raise "Commit transaction failed with error code: #{resp.error_code}"
      end

      @in_transaction = false
      @txn_partitions.clear
    end

    def abort_transaction
      tx_id = @transactional_id || raise "Not in transactional mode"
      unless @in_transaction
        raise "No transaction in progress"
      end

      client = @client || raise "Producer is closed"
      resp = client.end_txn(tx_id, client.producer_id, client.producer_epoch, false)
      if resp.error_code != 0
        raise "Abort transaction failed with error code: #{resp.error_code}"
      end

      @in_transaction = false
      @txn_partitions.clear
    end

    def send_offsets_to_transaction(offsets : Hash(String, Int64), group_id : String)
      tx_id = @transactional_id || raise "Not in transactional mode"
      unless @in_transaction
        raise "No transaction in progress"
      end

      client = @client || raise "Producer is closed"
      nested = {} of String => Hash(Int32, Int64)
      offsets.each do |key, offset|
        parts = key.split(":")
        topic = parts[0]
        partition = parts.size > 1 ? parts[1].to_i : 0
        nested[topic] ||= {} of Int32 => Int64
        nested[topic][partition] = offset
      end

      resp = client.txn_offset_commit(tx_id, group_id, client.producer_id, client.producer_epoch, nested)
      if resp.error_code != 0
        raise "Sending offsets to transaction failed with error code: #{resp.error_code}"
      end
    end

    def produce(topic : String, payload : Protocol::BytesOrString, key : Protocol::BytesOrString? = nil, headers : Array(Protocol::RecordHeader)? = nil, partition : Int32 = 0, timestamp : Time? = nil)
      client = @client || raise "Producer is closed"

      if @in_transaction && (tx_id = @transactional_id)
        slot = "#{topic}:#{partition}"
        unless @txn_partitions.includes?(slot)
          client.add_partitions_to_txn(tx_id, client.producer_id, client.producer_epoch, {topic => [partition]})
          @txn_partitions.add(slot)
        end
      end

      retries = (@config.settings["retries"]? || "0").to_i
      backoff_ms = (@config.settings["retry.backoff.ms"]? || "100").to_i

      attempts = 0
      loop do
        begin
          client.batch_produce(topic, key, payload, partition: partition, headers: headers || [] of Protocol::RecordHeader, timestamp: timestamp)
          break
        rescue ex
          attempts += 1
          if attempts > retries
            raise ex
          end
          sleep backoff_ms.milliseconds
        end
      end
    end

    def produce(topic : String, payload : Protocol::BytesOrString, key : Protocol::BytesOrString? = nil, headers : Hash(String, String) = {} of String => String, partition : Int32 = 0, timestamp : Time? = nil)
      record_headers = nil
      unless headers.empty?
        record_headers = Array(Protocol::RecordHeader).new(headers.size)
        headers.each do |k, v|
          record_headers << Protocol::RecordHeader.new(k, v)
        end
      end
      produce(topic, payload, key, headers: record_headers, partition: partition, timestamp: timestamp)
    end

    def flush(timeout_ms : Int32 = 5000)
      if client = @client
        client.flush_batch
      end
    end

    def on_deliver(&block : String, Int32, Int64, Exception? -> Void)
      if client = @client
        client.on_deliver = block
      end
    end

    def on_stats(&block : String -> Void)
      if client = @client
        client.on_stats(&block)
      end
    end

    def close
      if client = @client
        client.close
        @client = nil
      end
    end
  end
end
