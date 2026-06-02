require "uuid"

module Kafkaesque
  class Consumer
    class Config
      property bootstrap_servers : Array(String)
      property settings : Hash(String, String)
      property oauth_token_provider : (-> String)? = nil
      property sasl_token : String? = nil
      property initial_offset_smallest : Bool = false

      def initialize(
        @bootstrap_servers,
        group_id : String? = nil,
        @sasl_token : String? = nil,
        @initial_offset_smallest : Bool = false,
        @settings = {} of String => String,
      )
        if group_id
          set("group.id", group_id)
        end
        setup_oauth_provider
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

    @partition_offsets = Hash(Int32, Int64).new
    @offset_mutex = Mutex.new

    def initialize(@config : Config)
      @partition_offsets = Hash(Int32, Int64).new
      @offset_mutex = Mutex.new
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

    def each(&block : Protocol::Record ->)
      if @config.bootstrap_servers.empty?
        raise "No bootstrap servers configured"
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

      @offset_mutex.synchronize do
        @partition_offsets.clear
        @assigned_partitions.each do |part|
          @partition_offsets[part] = default_initial_offset
        end
      end

      # For each assigned partition, try to fetch the committed offset
      topic_name = @topics.first? || ""
      @hb_mutex.synchronize { @assigned_partitions.dup }.each do |part|
        begin
          fetch_resp = coord_client.offset_fetch(group_id, topic_name, part)
          if fetch_resp.error_code == 0 && fetch_resp.committed_offset >= 0
            @offset_mutex.synchronize do
              @partition_offsets[part] = fetch_resp.committed_offset
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
              offset = @offset_mutex.synchronize { @partition_offsets[part]? }
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

      while @running
        parts = @hb_mutex.synchronize { @assigned_partitions.dup }

        if parts.empty?
          sleep 200.milliseconds
          next
        end

        begin
          topic = @topics.first?
          break if topic.nil?

          # Fetch from all assigned partitions, not just the first
          parts.each do |part|
            next unless @running
            part_offset = @offset_mutex.synchronize { @partition_offsets[part]? } || default_initial_offset

            poll_resp = coord_client.fetch(topic, partition: part, fetch_offset: part_offset, min_bytes: fetch_min_bytes)
            if poll_resp.error_code == 0
              poll_resp.records.each do |record|
                break unless @running
                block.call(record)
                @offset_mutex.synchronize do
                  @partition_offsets[part] = record.offset + 1
                end
              end
            elsif poll_resp.error_code == 1
              # OFFSET_OUT_OF_RANGE — reset to actual earliest available offset
              Log.debug { "OFFSET_OUT_OF_RANGE on partition #{part} at offset #{part_offset}, querying earliest..." }
              begin
                list_resp = coord_client.list_offsets(topic, part, Client::TIMESTAMP_EARLIEST)
                if list_resp.error_code == 0 && list_resp.offset >= 0
                  Log.debug { "Resetting partition #{part} offset to #{list_resp.offset}" }
                  @offset_mutex.synchronize do
                    @partition_offsets[part] = list_resp.offset
                  end
                end
              rescue ex
                Log.error(exception: ex) { "list_offsets error on partition #{part}" }
              end
            else
              Log.debug { "Fetch error on partition #{part}: code=#{poll_resp.error_code}" }
            end
          end
        rescue ex
          Log.error(exception: ex) { "Consumer loop error" }
          sleep 1.second
        end

        sleep 100.milliseconds
      end
    ensure
      close_internal
    end

    private def resolve_coordinator(group_id : String) : Client
      attempts = 0
      loop do
        bootstrap_client = Client.connect_first(
          servers: @config.bootstrap_servers,
          sasl_token: @config.sasl_token,
          client_id: "kafkaesque-consumer-bootstrap",
          oauth_token_provider: @config.oauth_token_provider
        )

        coord_resp = bootstrap_client.find_coordinator(group_id)
        bootstrap_client.close

        if coord_resp.error_code == 0
          coord_client = Client.new(
            host: coord_resp.host,
            port: coord_resp.port,
            sasl_token: @config.sasl_token,
            client_id: "kafkaesque-consumer",
            oauth_token_provider: @config.oauth_token_provider
          )
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

        if @assigned_partitions != new_partitions
          if cb_rev = @on_partitions_revoked
            cb_rev.call(@assigned_partitions)
          end

          # Initialize offset for new partitions
          is_smallest = @config.initial_offset_smallest || @config.settings["auto.offset.reset"]? == "smallest"
          default_initial_offset = is_smallest ? 0_i64 : -1_i64

          @offset_mutex.synchronize do
            # Keep offsets for partitions that are still assigned, initialize new ones
            new_offsets = Hash(Int32, Int64).new
            new_partitions.each do |part|
              new_offsets[part] = @partition_offsets[part]? || default_initial_offset
            end
            @partition_offsets = new_offsets
          end

          # If coordinator client is already connected, try fetching committed offsets for new partitions
          if coord = @coordinator_client
            topic_name = @topics.first? || ""
            group_id = @config.settings["group.id"]? || "default-group"
            new_partitions.each do |part|
              # Only fetch if it was not already tracked/valid
              next if @partition_offsets[part]? && @partition_offsets[part] >= 0_i64
              begin
                fetch_resp = coord.offset_fetch(group_id, topic_name, part)
                if fetch_resp.error_code == 0 && fetch_resp.committed_offset >= 0
                  @offset_mutex.synchronize do
                    @partition_offsets[part] = fetch_resp.committed_offset
                  end
                  Log.debug { "Resuming partition #{part} from committed offset #{fetch_resp.committed_offset}" }
                end
              rescue ex
                # Ignore fetch error
              end
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

    def close
      @running = false
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
    end
  end
end
