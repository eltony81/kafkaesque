require "uuid"

module Kafkaesque
  class Consumer
    class Config
      property bootstrap_servers : Array(String)
      property settings : Hash(String, String)
      property oauth_token_provider : (-> String)? = nil
      property sasl_token : String? = nil
      property initial_offset_smallest : Bool = false
      property client_rack : String? = nil

      def initialize(
        bootstrap_servers : Array(String) = ["localhost:9092"],
        group_id : String? = nil,
        sasl_token : String? = nil,
        initial_offset_smallest : Bool = false,
        client_rack : String? = nil,
        settings = {} of String => String,
      )
        @bootstrap_servers = bootstrap_servers
        @sasl_token = sasl_token
        @initial_offset_smallest = initial_offset_smallest
        @settings = settings
        @client_rack = client_rack || settings["client.rack"]?
        if group_id
          set("group.id", group_id)
        end
        setup_oauth_provider
      end

      def self.build(&block : Config ->)
        cfg = new(bootstrap_servers: [] of String)
        block.call(cfg)
        cfg
      end

      def group_id=(val : String)
        set("group.id", val)
      end

      def group_id : String
        @settings["group.id"]? || "default-group"
      end

      def auto_commit=(val : Bool)
        set("enable.auto.commit", val.to_s)
      end

      def auto_commit : Bool
        @settings["enable.auto.commit"]? != "false"
      end

      def auto_commit_interval_ms=(val : Int32)
        set("auto.commit.interval.ms", val.to_s)
      end

      def auto_commit_interval_ms : Int32
        @settings["auto.commit.interval.ms"]?.try(&.to_i) || 5000
      end

      def set(key : String, value : String)
        @settings[key] = value
        setup_oauth_provider if key.starts_with?("sasl.oauthbearer")
      end

      def sasl_token
        @sasl_token || @settings["sasl.token"]? || @settings["sasl.password"]?
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
    getter subscription_pattern : Regex? = nil
    getter assigned_partitions = [] of Int32
    @topics = [] of String
    @running = true
    @client : Client?
    @coordinator_client : Client?
    @heartbeat_fiber : Fiber?
    @hb_mutex = Mutex.new

    # State variables for KIP-848
    @member_id = UUID.random.to_s
    @member_epoch = 0
    @topic_uuid = Bytes.empty

    @on_partitions_assigned : (Array(Int32) -> Void)? = nil
    @on_partitions_revoked : (Array(Int32) -> Void)? = nil

    @partition_offsets = Hash(Tuple(String, Int32), Int64).new
    @offset_mutex = Mutex.new

    @prefetch_channel : Channel(Array(Protocol::Record))
    @active_fetchers : Hash(Tuple(String, Int32), Bool)

    def self.new(&block : Config ->)
      cfg = Config.build(&block)
      new(cfg)
    end

    def initialize(@config : Config)
      @partition_offsets = Hash(Tuple(String, Int32), Int64).new
      @offset_mutex = Mutex.new
      @prefetch_channel = Channel(Array(Protocol::Record)).new(100)
      @active_fetchers = Hash(Tuple(String, Int32), Bool).new
    end

    def on_partitions_assigned(&block : Array(Int32) -> Void)
      @on_partitions_assigned = block
    end

    def on_partitions_revoked(&block : Array(Int32) -> Void)
      @on_partitions_revoked = block
    end

    def subscribe(topics : Array(String))
      @topics = topics
    end

    def subscribe(*topics : String)
      subscribe(topics.to_a)
    end

    def subscribe(pattern : Regex)
      @subscription_pattern = pattern
    end

    # Explicit manual partition assignments
    getter manual_assignments : Array(TopicPartition)? = nil

    # Assigns the consumer to a list of topic-partition pairs manually.
    # Bypasses group coordination and heartbeat loops.
    def assign(topic_partitions : Array(TopicPartition))
      @topics.clear
      @subscription_pattern = nil
      @manual_assignments = topic_partitions
    end

    def assign(topic_partition : TopicPartition)
      assign([topic_partition])
    end

    def share_each(&block : Protocol::Record ->)
      if @config.bootstrap_servers.empty?
        raise "No bootstrap servers configured"
      end

      max_retries = (@config.settings["retries"]? || @config.settings["max_retries"]?).try(&.to_i) || 3
      client = Client.connect_first(
        servers: @config.bootstrap_servers,
        sasl_token: @config.sasl_token,
        client_id: @config.settings["client.id"]? || "kafkaesque-share-consumer",
        oauth_token_provider: @config.oauth_token_provider,
        max_retries: max_retries,
        settings: @config.settings
      )
      client.client_rack = @config.client_rack
      @client = client

      group_id = @config.settings["group.id"]? || "default-share-group"
      topic_name = @topics.first? || raise "No topics subscribed for share group consume"

      Log.debug { "Starting Share Group consumer loop for group: #{group_id}, topic: #{topic_name}" }

      while @running
        begin
          resp = client.share_fetch(group_id, @member_id, topic_name, 0)
          if resp.error_code == 0
            resp.topics.each do |t|
              t.partitions.each do |p|
                p.records.each do |record|
                  block.call(record)
                  client.share_acknowledge(
                    group_id: group_id,
                    member_id: @member_id,
                    topic: t.name,
                    partition: p.partition_index,
                    first_offset: record.offset,
                    last_offset: record.offset,
                    ack_type: 1_i8
                  )
                end
              end
            end
          end
          sleep 100.milliseconds if resp.topics.all? { |t| t.partitions.all? &.records.empty? }
        rescue ex
          Log.warn { "Share Group fetch/ack error: #{ex.message}" }
          sleep 1.second
        end
      end
    ensure
      client.try(&.close)
    end

    def each(&block : Protocol::Record ->)
      if @config.bootstrap_servers.empty?
        raise "No bootstrap servers configured"
      end

      if pattern = @subscription_pattern
        resolve_regex_topics(pattern)
        spawn_regex_monitor_loop(pattern)
      end

      is_smallest = @config.initial_offset_smallest || @config.settings["auto.offset.reset"]? == "smallest"
      default_initial_offset = is_smallest ? 0_i64 : -1_i64

      if assignments = @manual_assignments
        max_retries = (@config.settings["retries"]? || @config.settings["max_retries"]?).try(&.to_i) || 3
        client = Client.connect_first(
          servers: @config.bootstrap_servers,
          sasl_token: @config.sasl_token,
          client_id: @config.settings["client.id"]? || "kafkaesque-consumer-manual",
          oauth_token_provider: @config.oauth_token_provider,
          max_retries: max_retries,
          settings: @config.settings
        )
        client.client_rack = @config.client_rack
        @client = client

        @assigned_partitions = assignments.map(&.partition).uniq

        @offset_mutex.synchronize do
          @partition_offsets.clear
          assignments.each do |tp|
            @partition_offsets[{tp.topic, tp.partition}] = default_initial_offset
          end
        end

        if group_id_present = @config.settings["group.id"]?
          begin
            coord_client = resolve_coordinator(group_id_present)
            assignments.each do |tp|
              fetch_resp = coord_client.offset_fetch(group_id_present, tp.topic, tp.partition)
              if fetch_resp.error_code == 0 && fetch_resp.committed_offset >= 0
                @offset_mutex.synchronize do
                  @partition_offsets[{tp.topic, tp.partition}] = fetch_resp.committed_offset
                end
              end
            end
            coord_client.close rescue nil
          rescue ex
            # fallback
          end
        end

        # Warm up partition metadata
        begin
          client.fetch_metadata(assignments.map(&.topic).uniq)
        rescue ex
          Log.debug { "Metadata prefetch warning: #{ex.message}" }
        end

        # Spawn background fetchers
        assignments.each do |tp|
          offset = @offset_mutex.synchronize { @partition_offsets[{tp.topic, tp.partition}]? } || default_initial_offset
          @active_fetchers[{tp.topic, tp.partition}] = true
          spawn_fetcher(tp.topic, tp.partition, offset, client)
        end

        # Stream records from prefetch queue
        begin
          while @running
            select
            when records = @prefetch_channel.receive
              records.each do |record|
                block.call(record)
              end
            end
          end
        rescue ex : Exception
          raise ex unless ex.is_a?(Channel::ClosedError)
        ensure
          close_internal
        end
        return
      end

      group_id = @config.settings["group.id"]? || "default-group"
      instance_id = @config.settings["group.instance.id"]?
      session_timeout = (@config.settings["session.timeout.ms"]? || "30000").to_i
      fetch_min_bytes = (@config.settings["fetch.min.bytes"]? || "1").to_i

      Log.debug { "Resolving coordinator for group #{group_id}..." }
      coord_client = resolve_coordinator(group_id)
      @coordinator_client = coord_client
      Log.debug { "Coordinator resolved. Host: #{coord_client.client_id}" }

      Log.debug { "Joining consumer group #{group_id}..." }
      hb_recommended_interval = join_consumer_group(coord_client, group_id, instance_id, session_timeout)
      hb_interval_ms = @config.settings["heartbeat.interval.ms"]?.try(&.to_i) || hb_recommended_interval
      Log.debug { "Joined group. Heartbeat interval: #{hb_interval_ms}ms" }

      # Wait for partition assignment — the broker may delay it to a 2nd heartbeat.
      # Poll additional heartbeats (up to 10 attempts, 1s apart) before starting the loop.
      if @assigned_partitions.empty?
        Log.debug { "No partitions assigned yet, polling for assignment..." }
        10.times do
          break unless @assigned_partitions.empty?
          sleep 1.second
          @hb_mutex.synchronize do
            begin
              owned_tp = [] of Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions
              r = coord_client.consumer_group_heartbeat(
                group_id: group_id,
                member_id: @member_id,
                member_epoch: @member_epoch,
                instance_id: instance_id,
                rebalance_timeout_ms: session_timeout,
                subscribed_topic_names: @topics,
                server_assignor: "uniform",
                topic_partitions: owned_tp
              )
              if r.error_code == 0
                @member_epoch = r.member_epoch
                apply_assignment(r)
              end
            rescue ex
              Log.debug { "Assignment poll heartbeat error: #{ex.message}" }
            end
          end
        end
      end

      spawn_heartbeat_loop(coord_client, hb_interval_ms)

      is_smallest = @config.initial_offset_smallest || @config.settings["auto.offset.reset"]? == "smallest"
      default_initial_offset = is_smallest ? 0_i64 : -1_i64
      topic_name = @topics.first? || ""

      @offset_mutex.synchronize do
        @partition_offsets.clear
        @assigned_partitions.each do |part|
          @partition_offsets[{topic_name, part}] = default_initial_offset
        end
      end

      # For each assigned partition, try to fetch the committed offset
      @hb_mutex.synchronize { @assigned_partitions.dup }.each do |part|
        begin
          fetch_resp = coord_client.offset_fetch(group_id, topic_name, part)
          if fetch_resp.error_code == 0 && fetch_resp.committed_offset >= 0
            @offset_mutex.synchronize do
              @partition_offsets[{topic_name, part}] = fetch_resp.committed_offset
            end
            Log.debug { "Resuming partition #{part} from committed offset #{fetch_resp.committed_offset}" }
          end
        rescue ex
          Log.debug { "Could not fetch committed offset for partition #{part}: #{ex.message}" }
        end
      end

      Log.debug { "Starting consumer loop with default offset #{default_initial_offset}... Assigned partitions: #{@assigned_partitions}" }

      # Warm up partition leader cache so connection_for_partition routes to the right broker
      begin
        coord_client.fetch_metadata([@topics.first?].compact)
      rescue ex
        Log.debug { "Metadata prefetch warning: #{ex.message}" }
      end

      # Spawn background fetchers for assigned partitions
      @hb_mutex.synchronize { @assigned_partitions.dup }.each do |part|
        offset = @offset_mutex.synchronize { @partition_offsets[{topic_name, part}]? } || default_initial_offset
        @active_fetchers[{topic_name, part}] = true
        spawn_fetcher(topic_name, part, offset, coord_client)
      end

      auto_commit = @config.settings["enable.auto.commit"]? != "false"
      auto_commit_interval = (@config.settings["auto.commit.interval.ms"]? || "5000").to_i

      if auto_commit
        spawn do
          while @running
            sleep auto_commit_interval.milliseconds
            break unless @running

            # Commit current offsets for all assigned partitions
            parts = @hb_mutex.synchronize { @assigned_partitions.dup }
            parts.each do |part|
              offset = @offset_mutex.synchronize { @partition_offsets[{topic_name, part}]? }
              next if offset.nil? || offset < 0_i64

              begin
                coord_client.offset_commit(
                  group_id: group_id,
                  generation_id: @member_epoch,
                  member_id: @member_id,
                  topic: topic_name,
                  partition: part,
                  offset: offset
                )
                Log.debug { "Auto committed partition #{part} offset to #{offset}" }
              rescue ex
                Log.error(exception: ex) { "Auto commit failed for partition #{part}" }
              end
            end
          end
        end
      end

      begin
        while @running
          select
          when records = @prefetch_channel.receive
            records.each do |record|
              block.call(record)
            end
          when timeout(200.milliseconds)
            Fiber.yield
          end
        end
      rescue ex : Exception
        raise ex unless ex.is_a?(Channel::ClosedError)
      ensure
        close_internal
      end
    end

    private def resolve_coordinator(group_id : String) : Client
      attempts = 0
      max_retries = (@config.settings["retries"]? || @config.settings["max_retries"]?).try(&.to_i) || 3
      loop do
        bootstrap_client = Client.connect_first(
          servers: @config.bootstrap_servers,
          sasl_token: @config.sasl_token,
          client_id: "kafkaesque-consumer-bootstrap",
          oauth_token_provider: @config.oauth_token_provider,
          max_retries: max_retries,
          settings: @config.settings
        )

        coord_resp = bootstrap_client.find_coordinator(group_id)
        bootstrap_client.close

        if coord_resp.error_code == 0
          coord_client = Client.new(
            host: coord_resp.host,
            port: coord_resp.port,
            sasl_token: @config.sasl_token,
            client_id: "kafkaesque-consumer",
            oauth_token_provider: @config.oauth_token_provider,
            max_retries: max_retries,
            settings: @config.settings
          )
          coord_client.client_rack = @config.client_rack
          coord_client.connect
          return coord_client
        elsif coord_resp.error_code == 15 && attempts < 5
          attempts += 1
          Log.debug { "Coordinator loading (15), retrying in 1s..." }
          sleep 1.second
        else
          raise "Failed to find group coordinator: error code #{coord_resp.error_code}"
        end
      end
    end

    private def join_consumer_group(coord_client : Client, group_id : String, instance_id : String?, session_timeout : Int32) : Int32
      hb_resp = coord_client.consumer_group_heartbeat(
        group_id: group_id,
        member_id: @member_id,
        member_epoch: @member_epoch,
        instance_id: instance_id,
        rebalance_timeout_ms: session_timeout,
        subscribed_topic_names: @topics,
        server_assignor: "uniform"
      )

      if hb_resp.error_code != 0
        raise "Failed to join consumer group: error #{hb_resp.error_code} (#{hb_resp.error_message})"
      end

      @member_id = hb_resp.member_id.to_s
      @member_epoch = hb_resp.member_epoch

      # Apply initial partition assignment if the broker sent it in the join response
      apply_assignment(hb_resp)

      hb_resp.heartbeat_interval_ms
    end

    # Extract and store partition assignment from a heartbeat response
    private def apply_assignment(r)
      if assign = r.assignment
        new_partitions = [] of Int32
        new_topic_uuid = Bytes.empty
        assign.topic_partitions.each do |tp|
          new_topic_uuid = tp.topic_id
          new_partitions.concat(tp.partitions)
        end

        topic_name = @topics.first? || ""

        if @assigned_partitions != new_partitions
          if cb_rev = @on_partitions_revoked
            cb_rev.call(@assigned_partitions)
          end

          # Initialize offset for new partitions
          is_smallest = @config.initial_offset_smallest || @config.settings["auto.offset.reset"]? == "smallest"
          default_initial_offset = is_smallest ? 0_i64 : -1_i64

          @offset_mutex.synchronize do
            # Keep offsets for partitions that are still assigned, initialize new ones
            new_offsets = Hash(Tuple(String, Int32), Int64).new
            new_partitions.each do |part|
              new_offsets[{topic_name, part}] = @partition_offsets[{topic_name, part}]? || default_initial_offset
            end
            @partition_offsets = new_offsets
          end

          # If coordinator client is already connected, try fetching committed offsets for new partitions
          if coord = @coordinator_client
            group_id = @config.settings["group.id"]? || "default-group"
            new_partitions.each do |part|
              # Only fetch if it was not already tracked/valid
              next if @partition_offsets[{topic_name, part}]? && @partition_offsets[{topic_name, part}] >= 0_i64
              begin
                fetch_resp = coord.offset_fetch(group_id, topic_name, part)
                if fetch_resp.error_code == 0 && fetch_resp.committed_offset >= 0
                  @offset_mutex.synchronize do
                    @partition_offsets[{topic_name, part}] = fetch_resp.committed_offset
                  end
                  Log.debug { "Resuming partition #{part} from committed offset #{fetch_resp.committed_offset}" }
                end
              rescue ex
                # Ignore fetch error
              end
            end
          end

          # Spawn fetchers for new partitions:
          if @running && !@prefetch_channel.closed? && (coord = @coordinator_client)
            (new_partitions - @assigned_partitions).each do |part|
              offset = @offset_mutex.synchronize { @partition_offsets[{topic_name, part}]? } || default_initial_offset
              @active_fetchers[{topic_name, part}] = true
              spawn_fetcher(topic_name, part, offset, coord)
            end
          end

          @topic_uuid = new_topic_uuid
          @assigned_partitions = new_partitions
          Log.debug { "Partition assignment applied: #{new_partitions}" }
          if cb_ass = @on_partitions_assigned
            cb_ass.call(new_partitions)
          end
        end
      end
    end

    private def spawn_fetcher(topic : String, partition : Int32, start_offset : Int64, client : Client)
      @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = start_offset }

      spawn do
        current_offset = start_offset
        fetch_min_bytes = (@config.settings["fetch.min.bytes"]? || "1").to_i

        while @running && @active_fetchers[{topic, partition}]?
          begin
            poll_resp = client.fetch(topic, partition: partition, fetch_offset: current_offset, min_bytes: fetch_min_bytes)
            if poll_resp.error_code == 0
              if poll_resp.records.empty?
                sleep 50.milliseconds
              else
                @prefetch_channel.send(poll_resp.records)
                if last_record = poll_resp.records.last?
                  current_offset = last_record.offset + 1
                  @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = current_offset }
                end
              end
            elsif poll_resp.error_code == 1
              Log.debug { "OFFSET_OUT_OF_RANGE on partition #{partition} at offset #{current_offset}, querying earliest..." }
              list_resp = client.list_offsets(topic, partition, Client::TIMESTAMP_EARLIEST)
              if list_resp.error_code == 0 && list_resp.offset >= 0
                current_offset = list_resp.offset
                @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = current_offset }
              else
                sleep 100.milliseconds
              end
            else
              sleep 100.milliseconds
            end
          rescue ex
            # Network or transport failure, backoff
            sleep 500.milliseconds
          end
        end
        @active_fetchers.delete({topic, partition})
      end
    end

    def close
      @running = false
      @active_fetchers.each_key do |part|
        @active_fetchers[part] = false
      end
      @prefetch_channel.close rescue nil
      close_internal
    end

    private def spawn_heartbeat_loop(client : Client, interval_ms : Int32)
      spawn do
        while @running
          sleep (interval_ms / 1000.0).seconds
          break unless @running

          @hb_mutex.synchronize do
            begin
              owned_tp = [] of Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions
              if !@assigned_partitions.empty? && !@topic_uuid.empty?
                owned_tp = [
                  Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions.new(@topic_uuid, @assigned_partitions),
                ]
              end

              group_id = @config.settings["group.id"]? || "default-group"
              instance_id = @config.settings["group.instance.id"]?
              session_timeout = (@config.settings["session.timeout.ms"]? || "30000").to_i

              r = client.consumer_group_heartbeat(
                group_id: group_id,
                member_id: @member_id,
                member_epoch: @member_epoch,
                instance_id: instance_id,
                rebalance_timeout_ms: session_timeout,
                subscribed_topic_names: @topics,
                server_assignor: "uniform",
                topic_partitions: owned_tp
              )

              if r.error_code == 0
                @member_epoch = r.member_epoch
                apply_assignment(r)
              end
            rescue ex
              Log.error(exception: ex) { "Background heartbeat error" }
            end
          end
        end
      end
    end

    private def close_internal
      # Leave the group cleanly by sending heartbeat with epoch = -1
      if !@assigned_partitions.empty?
        if cb_rev = @on_partitions_revoked
          cb_rev.call(@assigned_partitions)
        end
        @assigned_partitions = [] of Int32
      end
      if client = @coordinator_client
        @hb_mutex.synchronize do
          group_id = @config.settings["group.id"]? || "default-group"
          client.consumer_group_heartbeat(
            group_id: group_id,
            member_id: @member_id,
            member_epoch: -1
          ) rescue nil
        end
        client.close
        @coordinator_client = nil
      end
      if client = @client
        client.close rescue nil
        @client = nil
      end
    end

    private def resolve_regex_topics(pattern : Regex)
      max_retries = (@config.settings["retries"]? || @config.settings["max_retries"]?).try(&.to_i) || 3
      bootstrap_client = Client.connect_first(
        servers: @config.bootstrap_servers,
        sasl_token: @config.sasl_token,
        client_id: "kafkaesque-consumer-bootstrap",
        oauth_token_provider: @config.oauth_token_provider,
        max_retries: max_retries,
        settings: @config.settings
      )
      begin
        meta = bootstrap_client.fetch_metadata(nil)
        matched = [] of String
        meta.topics.each do |topic_meta|
          if topic_meta.name =~ pattern
            matched << topic_meta.name
          end
        end
        @topics = matched.sort
      ensure
        bootstrap_client.close rescue nil
      end
    end

    private def spawn_regex_monitor_loop(pattern : Regex)
      spawn do
        while @running
          sleep 10.seconds
          break unless @running
          begin
            old_topics = @topics
            resolve_regex_topics(pattern)
            if old_topics != @topics
              Log.info { "Regex subscription matched new topics: #{@topics}. Triggering membership update." }
            end
          rescue ex
            # ignore background connection errors
          end
        end
      end
    end

    def configurations : String
      String.build do |str|
        str << "Bootstrap Servers: #{@config.bootstrap_servers.join(", ")}\n"
        str << "Settings:\n"
        @config.settings.each do |k, v|
          str << "  #{k}: #{v}\n"
        end
      end
    end
  end
end
