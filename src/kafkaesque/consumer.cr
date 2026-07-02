require "uuid"
require "base64"

module Kafkaesque
  # Raised internally when the broker doesn't support KIP-848
  # ConsumerGroupHeartbeat (error UNSUPPORTED_VERSION) — signals
  # Consumer#each to fall back to the classic JoinGroup/SyncGroup protocol
  # rather than a hard failure.
  class UnsupportedGroupProtocolError < Exception
  end

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

      def fetch_max_bytes=(val : Int32)
        set("fetch.max.bytes", val.to_s)
      end

      def fetch_max_bytes : Int32
        @settings["fetch.max.bytes"]?.try(&.to_i) || 1048576
      end

      def max_partition_fetch_bytes=(val : Int32)
        set("max.partition.fetch.bytes", val.to_s)
      end

      def max_partition_fetch_bytes : Int32
        @settings["max.partition.fetch.bytes"]?.try(&.to_i) || 1048576
      end

      def metadata_refresh_interval_ms=(val : Int32)
        set("topic.metadata.refresh.interval.ms", val.to_s)
      end

      def metadata_refresh_interval_ms : Int32
        @settings["topic.metadata.refresh.interval.ms"]?.try(&.to_i) || 300000
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
    @initialized = false
    @closed = false
    @client : Client?
    @coordinator_client : Client?
    @heartbeat_fiber : Fiber?
    @hb_mutex = Mutex.new

    # State variables for KIP-848
    @member_id = UUID.random.to_s
    @member_epoch = 0
    @topic_uuid = Bytes.empty

    # Set when #each fell back to the classic JoinGroup/SyncGroup consumer
    # group protocol (broker doesn't support KIP-848 ConsumerGroupHeartbeat).
    # @member_epoch doubles as the classic protocol's generation_id in this
    # mode — the two protocols are never active at once for a given #each call.
    @using_classic_group_protocol = false

    # Set when the group-managed #each flow (KIP-848 or classic fallback) is
    # using the single consolidated KIP-227 fetch loop (#spawn_multi_fetcher)
    # instead of a fiber-per-partition. Manual partition assignment still
    # uses the per-partition model (it can span multiple topics, which
    # Client#fetch_many doesn't support), so #apply_assignment must not spawn
    # per-partition fetchers when this is set — the consolidated loop already
    # re-reads @assigned_partitions every tick.
    @use_batched_fetch = false

    @on_partitions_assigned : (Array(Int32) -> Void)? = nil
    @on_partitions_revoked : (Array(Int32) -> Void)? = nil
    @on_consume : (Protocol::Record -> Void)? = nil

    @partition_offsets = Hash(Tuple(String, Int32), Int64).new
    @offset_mutex = Mutex.new
    @fetcher_mutex = Mutex.new

    @prefetch_channel : Channel(Array(Protocol::Record))
    @active_fetchers : Hash(Tuple(String, Int32), Bool)

    def self.new(&block : Config ->)
      cfg = Config.build(&block)
      new(cfg)
    end

    @paused_partitions = Set(Tuple(String, Int32)).new

    def initialize(@config : Config)
      @partition_offsets = Hash(Tuple(String, Int32), Int64).new
      @offset_mutex = Mutex.new
      @fetcher_mutex = Mutex.new
      @prefetch_channel = Channel(Array(Protocol::Record)).new(100)
      @active_fetchers = Hash(Tuple(String, Int32), Bool).new
      @paused_partitions = Set(Tuple(String, Int32)).new
      @initialized = false
      @closed = false
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

    def pause(topic : String, partition : Int32)
      @paused_partitions.add({topic, partition})
      Log.debug { "Paused fetching for partition #{topic}:#{partition}" }
    end

    def pause(topic_partitions : Array(TopicPartition))
      topic_partitions.each { |tp| pause(tp.topic, tp.partition) }
    end

    def resume(topic : String, partition : Int32)
      @paused_partitions.delete({topic, partition})
      Log.debug { "Resumed fetching for partition #{topic}:#{partition}" }
    end

    def resume(topic_partitions : Array(TopicPartition))
      topic_partitions.each { |tp| resume(tp.topic, tp.partition) }
    end

    def paused?(topic : String, partition : Int32) : Bool
      @paused_partitions.includes?({topic, partition})
    end

    def commit(offsets : Hash(TopicPartition, Int64))
      coord = @coordinator_client || @client || raise "Consumer is not currently connected to any coordinator or broker"
      group_id = @config.settings["group.id"]? || "default-group"
      offsets.each do |tp, offset|
        coord.offset_commit(
          group_id: group_id,
          generation_id: @member_epoch,
          member_id: @member_id,
          topic: tp.topic,
          partition: tp.partition,
          offset: offset
        )
      end
    end

    def commit_async(offsets : Hash(TopicPartition, Int64))
      spawn do
        commit(offsets)
      rescue ex
        Log.error(exception: ex) { "Async manual offset commit failed" }
      end
    end

    def share_each(&block : Protocol::Record ->)
      if @config.bootstrap_servers.empty?
        raise "No bootstrap servers configured"
      end

      client : Client? = nil
      max_retries = (@config.settings["retries"]? || @config.settings["max_retries"]?).try(&.to_i) || 3
      group_id = @config.settings["group.id"]? || "default-share-group"
      topic_name = @topics.first? || raise "No topics subscribed for share group consume"

      # KIP-932's ShareFetch/ShareAcknowledge v1 parse MemberId with Kafka's
      # Uuid.fromString() — a URL-safe, unpadded Base64 encoding of 16 raw
      # bytes (22 chars) — NOT an arbitrary string like classic consumer-group
      # member IDs (@member_id, a hyphenated UUID string, would fail to parse
      # here). Generate a dedicated Kafka-format member ID for this share
      # session; ShareGroupHeartbeat accepts either form, so it's used
      # consistently across heartbeat/fetch/acknowledge.
      share_member_id = Base64.urlsafe_encode(Random::Secure.random_bytes(16), padding: false)

      # Share-group membership (ShareGroupHeartbeat) is resolved via the same
      # classic GROUP-type FindCoordinator (key=group_id) as KIP-848 consumer
      # groups — confirmed against the real client: RequestManagers.java builds
      # ShareHeartbeatRequestManager on top of a plain CoordinatorRequestManager
      # (CoordinatorType.GROUP), not a SHARE-typed lookup. ShareFetch/
      # ShareAcknowledge then route to each partition's own leader (like
      # regular Fetch), unaffected by this.
      client = resolve_coordinator(group_id)
      client.client_rack = @config.client_rack
      @client = client

      Log.debug { "Starting Share Group consumer loop for group: #{group_id}, topic: #{topic_name}" }

      # Warm up partition/topic-id metadata (Metadata v12+, KIP-516) — ShareFetch
      # v1 addresses topics by ID. connection_for_partition (not the lower-level
      # fetch_metadata) is what actually populates the @topic_ids/
      # @partition_leaders cache on a miss.
      begin
        client.connection_for_partition(topic_name, 0)
      rescue ex
        Log.debug { "Metadata prefetch warning: #{ex.message}" }
      end
      topic_id = client.topic_id_for(topic_name)

      assigned_partitions = [] of Int32

      hb_resp = client.share_group_heartbeat(
        group_id: group_id,
        member_id: share_member_id,
        member_epoch: 0,
        rack_id: @config.client_rack,
        subscribed_topic_names: [topic_name]
      )
      if hb_resp.error_code != 0
        raise "Failed to join share group: error #{hb_resp.error_code} (#{hb_resp.error_message})"
      end
      @member_epoch = hb_resp.member_epoch
      if initial_assignment = hb_resp.assignment
        initial_assignment.topic_partitions.each { |tp| assigned_partitions.concat(tp.partitions) }
      end
      # Fall back to every partition of the topic if the broker left the assignment
      # unset on the initial heartbeat (some brokers only send it on the next one).
      if assigned_partitions.empty?
        assigned_partitions = (0...client.partitions_count(topic_name)).to_a
      end
      assigned_partitions.uniq!

      hb_interval_ms = hb_resp.heartbeat_interval_ms > 0 ? hb_resp.heartbeat_interval_ms : 5000

      spawn do
        while @running
          sleep (hb_interval_ms / 1000.0).seconds
          break unless @running
          begin
            r = client.share_group_heartbeat(
              group_id: group_id,
              member_id: share_member_id,
              member_epoch: @member_epoch,
              subscribed_topic_names: [topic_name]
            )
            if r.error_code == 0
              @member_epoch = r.member_epoch
              if renewed_assignment = r.assignment
                new_partitions = [] of Int32
                renewed_assignment.topic_partitions.each { |tp| new_partitions.concat(tp.partitions) }
                assigned_partitions = new_partitions.uniq unless new_partitions.empty?
              end
            end
          rescue ex
            Log.warn { "Share group heartbeat error: #{ex.message}" }
          end
        end
      end

      # KIP-932 v1 share sessions work like incremental Fetch sessions
      # (KIP-227): 0 opens/resets the session, it then increments by 1 on each
      # subsequent request, and -1 closes it. Acknowledgements for records
      # delivered from the PREVIOUS response are piggybacked on the NEXT
      # ShareFetchRequest rather than requiring a separate round trip.
      share_session_epoch = 0
      pending_acks = Hash(Int32, Array(Protocol::ShareAcknowledgementBatch)).new
      require_topic_id = topic_id || raise "No topic id available for '#{topic_name}'; cannot use ShareFetch v1 (requires Metadata v12+/KIP-516)"

      while @running
        begin
          if assigned_partitions.empty?
            sleep 200.milliseconds
            next
          end

          resp = client.share_fetch(group_id, share_member_id, topic_name, require_topic_id, assigned_partitions, share_session_epoch, pending_acks)
          pending_acks = Hash(Int32, Array(Protocol::ShareAcknowledgementBatch)).new

          case resp.error_code
          when 0
            share_session_epoch = share_session_epoch == 0 ? 1 : share_session_epoch + 1
          when 122, 123 # SHARE_SESSION_NOT_FOUND, INVALID_SHARE_SESSION_EPOCH — reopen the session
            Log.warn { "Share session error #{resp.error_code} (#{resp.error_message}), reopening session" }
            share_session_epoch = 0
          else
            Log.warn { "ShareFetch error #{resp.error_code}: #{resp.error_message}" }
          end

          any_records = false
          if resp.error_code == 0
            resp.topics.each do |t|
              t.partitions.each do |p|
                p.acquired_records.each do |acquired|
                  (pending_acks[p.partition_index] ||= [] of Protocol::ShareAcknowledgementBatch) <<
                    Protocol::ShareAcknowledgementBatch.new(acquired.first_offset, acquired.last_offset, [1_i8]) # Accept
                end
                next if p.records.empty?
                any_records = true
                p.records.each do |record|
                  if cb = @on_consume
                    cb.call(record)
                  end
                  block.call(record)
                end
              end
            end
          end
          sleep 100.milliseconds unless any_records
        rescue ex
          Log.warn { "Share Group fetch/ack error: #{ex.message}" }
          sleep 1.second
        end
      end

      # No explicit "flush remaining acks + close session" round trip here:
      # acknowledgements for the last batch delivered before #close would
      # otherwise require one more blocking network call on the way out.
      # It isn't needed for correctness — this is at-least-once delivery, and
      # the broker reclaims any acquired-but-unacknowledged records for
      # redelivery once their acquisition lock expires, exactly as documented
      # for an ungracefully-closed share session.


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
          spawn_fetcher(tp.topic, tp.partition, offset, client)
        end

        # Stream records from prefetch queue
        begin
          while @running
            records = @prefetch_channel.receive
            records.each do |record|
              if cb = @on_consume
                cb.call(record)
              end
              block.call(record)
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
      begin
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
                assignor = @config.settings["group.remote.assignor"]? || "uniform"
                r = coord_client.consumer_group_heartbeat(
                  group_id: group_id,
                  member_id: @member_id,
                  member_epoch: @member_epoch,
                  instance_id: instance_id,
                  rebalance_timeout_ms: session_timeout,
                  subscribed_topic_names: @topics,
                  server_assignor: assignor,
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
      rescue unsupported_ex : UnsupportedGroupProtocolError
        Log.debug { "#{unsupported_ex.message}; falling back to the classic JoinGroup/SyncGroup consumer group protocol" }
        @using_classic_group_protocol = true
        hb_interval_ms = join_classic_consumer_group(coord_client, group_id, session_timeout)
        Log.debug { "Joined group via classic protocol. Heartbeat interval: #{hb_interval_ms}ms" }
        spawn_classic_heartbeat_loop(coord_client, group_id, hb_interval_ms)
      end

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

      # KIP-227: one consolidated fetch loop for all assigned partitions of
      # this topic (grouped per-broker, session-tracked — see
      # Client#fetch_many) rather than a fiber-per-partition, each sending
      # its own independent FetchRequest. Re-reads @assigned_partitions
      # every tick, so it naturally picks up/drops partitions across
      # rebalances without needing per-partition fiber bookkeeping.
      @use_batched_fetch = true
      spawn_multi_fetcher(topic_name, coord_client)

      @initialized = true

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
          records = @prefetch_channel.receive
          records.each do |record|
            if cb = @on_consume
              cb.call(record)
            end
            block.call(record)
          end
        end
      rescue ex : Exception
        raise ex unless ex.is_a?(Channel::ClosedError)
      ensure
        close_internal
      end
    end

    def on_consume(&block : Protocol::Record -> Void)
      @on_consume = block
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
      assignor = @config.settings["group.remote.assignor"]? || "uniform"
      hb_resp = coord_client.consumer_group_heartbeat(
        group_id: group_id,
        member_id: @member_id,
        member_epoch: @member_epoch,
        instance_id: instance_id,
        rebalance_timeout_ms: session_timeout,
        subscribed_topic_names: @topics,
        server_assignor: assignor
      )

      if hb_resp.error_code == 35 # UNSUPPORTED_VERSION
        raise UnsupportedGroupProtocolError.new("Broker does not support KIP-848 ConsumerGroupHeartbeat (error 35)")
      end
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

          unless @use_batched_fetch
            # Spawn fetchers for new partitions:
            if @initialized && @running && !@prefetch_channel.closed? && (coord = @coordinator_client)
              (new_partitions - @assigned_partitions).each do |part|
                offset = @offset_mutex.synchronize { @partition_offsets[{topic_name, part}]? } || default_initial_offset
                spawn_fetcher(topic_name, part, offset, coord)
              end
            end

            # Stop fetchers for partitions revoked in this rebalance — otherwise their
            # background fibers keep fetching and delivering records for a partition
            # this member no longer owns, causing duplicate delivery once another
            # group member picks it up.
            (@assigned_partitions - new_partitions).each do |part|
              @fetcher_mutex.synchronize { @active_fetchers[{topic_name, part}] = false }
            end
          end
          # Under @use_batched_fetch, #spawn_multi_fetcher re-reads
          # @assigned_partitions (set below) every tick — new/revoked
          # partitions are picked up automatically, no per-partition fiber
          # bookkeeping needed.

          @topic_uuid = new_topic_uuid
          @assigned_partitions = new_partitions
          Log.debug { "Partition assignment applied: #{new_partitions}" }
          if cb_ass = @on_partitions_assigned
            cb_ass.call(new_partitions)
          end
        end
      end
    end

    # Classic consumer group protocol (JoinGroup/SyncGroup) fallback for
    # brokers that don't support KIP-848 ConsumerGroupHeartbeat (Kafka < 3.7,
    # or `group.coordinator.rebalance.protocols` without `consumer`). Unlike
    # KIP-848 — where the broker computes assignments server-side — here the
    # elected group *leader* computes assignments for every member itself
    # (via RangeAssignor, matching classic Kafka's default) and submits them
    # through SyncGroup; followers just read back what the leader assigned
    # them. Returns the heartbeat interval to use (classic JoinGroup doesn't
    # advertise one, so this follows the conventional session_timeout/3).
    private def join_classic_consumer_group(coord_client : Client, group_id : String, session_timeout : Int32) : Int32
      join_resp = coord_client.join_group(group_id, @member_id, @topics)
      if join_resp.error_code != 0
        raise "Failed to join classic consumer group: error #{join_resp.error_code}"
      end

      @member_id = join_resp.member_id
      generation_id = join_resp.generation_id

      group_assignments = {} of String => Bytes
      if join_resp.leader?
        members_topics = join_resp.members.transform_values do |metadata|
          Protocol::ConsumerProtocolSubscription.deserialize(metadata).topics
        end
        subscribed_topics = members_topics.values.flatten.uniq
        partitions_by_topic = subscribed_topics.to_h { |t| {t, coord_client.partitions_count(t)} }

        assignment_by_member = Protocol::RangeAssignor.assign(members_topics, partitions_by_topic)
        assignment_by_member.each do |mid, assigned|
          io = IO::Memory.new
          Protocol::ConsumerProtocolAssignment.new(assigned).serialize(io)
          group_assignments[mid] = io.to_slice
        end
        Log.debug { "Elected leader for classic group #{group_id}; computed assignments for #{assignment_by_member.size} member(s)" }
      end

      sync_resp = coord_client.sync_group(group_id, generation_id, @member_id, group_assignments)
      if sync_resp.error_code != 0
        raise "Failed to sync classic consumer group: error #{sync_resp.error_code}"
      end

      @member_epoch = generation_id
      topic_name = @topics.first? || ""
      assigned = Protocol::ConsumerProtocolAssignment.deserialize(sync_resp.assignment || Bytes.empty)
      @assigned_partitions = assigned.assigned_partitions[topic_name]? || [] of Int32
      Log.debug { "Classic assignment applied: #{@assigned_partitions}" }
      if cb_ass = @on_partitions_assigned
        cb_ass.call(@assigned_partitions)
      end

      (session_timeout // 3).clamp(1000, session_timeout)
    end

    private def spawn_classic_heartbeat_loop(client : Client, group_id : String, interval_ms : Int32)
      spawn do
        while @running
          sleep (interval_ms / 1000.0).seconds
          break unless @running

          member_id, generation_id = @hb_mutex.synchronize { {@member_id, @member_epoch} }

          begin
            r = client.heartbeat(group_id, generation_id, member_id)
            case r.error_code
            when 0
              # steady state, nothing to do
            when 27 # REBALANCE_IN_PROGRESS
              Log.debug { "Classic group rebalance in progress; rejoining..." }
              session_timeout = (@config.settings["session.timeout.ms"]? || "30000").to_i
              @hb_mutex.synchronize do
                begin
                  join_classic_consumer_group(client, group_id, session_timeout)
                rescue ex
                  Log.warn { "Classic rejoin failed: #{ex.message}" }
                end
              end
            else
              Log.warn { "Classic group heartbeat error #{r.error_code}" }
            end
          rescue ex
            Log.warn { "Classic group heartbeat failed: #{ex.message}" }
          end
        end
      end
    end

    private def spawn_fetcher(topic : String, partition : Int32, start_offset : Int64, client : Client)
      @fetcher_mutex.synchronize do
        if @active_fetchers[{topic, partition}]?
          @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = start_offset }
          return
        end
        @active_fetchers[{topic, partition}] = true
      end
      @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = start_offset }

      spawn do
        current_offset = start_offset
        fetch_min_bytes = (@config.settings["fetch.min.bytes"]? || "1").to_i

        while @running && @fetcher_mutex.synchronize { @active_fetchers[{topic, partition}]? }
          if paused?(topic, partition)
            sleep 100.milliseconds
            next
          end

          begin
            poll_resp = client.fetch(topic, partition: partition, fetch_offset: current_offset, min_bytes: fetch_min_bytes)
            if poll_resp.error_code == 0
              if poll_resp.records.empty?
                sleep 50.milliseconds
              else
                # Re-check paused state AFTER the (potentially long-blocking) fetch
                # returns. If pause() was called while the fetch was in-flight we
                # must NOT deliver the records — discard them and do NOT advance the
                # offset so they will be re-fetched once the partition is resumed.
                if paused?(topic, partition)
                  Log.debug { "Discarding #{poll_resp.records.size} in-flight records for paused partition #{topic}:#{partition}" }
                  sleep 100.milliseconds
                else
                  valid_records = poll_resp.records.select { |r| r.offset >= current_offset }
                  unless valid_records.empty?
                    @prefetch_channel.send(valid_records)
                  end
                  if last_record = poll_resp.records.last?
                    next_offset = last_record.offset + 1
                    current_offset = next_offset if next_offset > current_offset
                    @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = current_offset }
                  end
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
        @fetcher_mutex.synchronize { @active_fetchers.delete({topic, partition}) }
      end
    end

    # KIP-227 consolidated fetch loop for the group-managed flow (see
    # @use_batched_fetch): fetches every currently-assigned, non-paused
    # partition of `topic` in one Client#fetch_many call per tick instead of
    # a fiber-per-partition. Re-reads @assigned_partitions every iteration,
    # so partition churn from rebalances is picked up automatically.
    private def spawn_multi_fetcher(topic : String, client : Client)
      fetch_min_bytes = (@config.settings["fetch.min.bytes"]? || "1").to_i

      spawn do
        while @running
          parts = @hb_mutex.synchronize { @assigned_partitions.dup }
          active_parts = parts.reject { |p| paused?(topic, p) }

          if active_parts.empty?
            sleep 100.milliseconds
            next
          end

          offsets = @offset_mutex.synchronize do
            active_parts.to_h { |p| {p, @partition_offsets[{topic, p}]? || -1_i64} }
          end

          begin
            results = client.fetch_many(topic, offsets, min_bytes: fetch_min_bytes)
            any_records = false

            results.each do |partition, result|
              current_offset = offsets[partition]?
              next unless current_offset

              case result.error_code
              when 0
                # Re-check paused state AFTER the (potentially long-blocking) fetch
                # returns — if pause() was called mid-flight, discard these records
                # and do NOT advance the offset so they're re-fetched once resumed.
                if paused?(topic, partition)
                  Log.debug { "Discarding #{result.records.size} in-flight records for paused partition #{topic}:#{partition}" }
                  next
                end

                valid_records = result.records.select { |r| r.offset >= current_offset }
                next if valid_records.empty?
                any_records = true
                @prefetch_channel.send(valid_records)
                if last_record = valid_records.last?
                  next_offset = last_record.offset + 1
                  @offset_mutex.synchronize do
                    if next_offset > (@partition_offsets[{topic, partition}]? || -1_i64)
                      @partition_offsets[{topic, partition}] = next_offset
                    end
                  end
                end
              when 1 # OFFSET_OUT_OF_RANGE
                Log.debug { "OFFSET_OUT_OF_RANGE on partition #{partition} at offset #{current_offset}, querying earliest..." }
                begin
                  list_resp = client.list_offsets(topic, partition, Client::TIMESTAMP_EARLIEST)
                  if list_resp.error_code == 0 && list_resp.offset >= 0
                    @offset_mutex.synchronize { @partition_offsets[{topic, partition}] = list_resp.offset }
                  end
                rescue ex
                  Log.debug { "list_offsets fallback failed for partition #{partition}: #{ex.message}" }
                end
              else
                Log.debug { "Batched fetch error #{result.error_code} on partition #{partition}" }
              end
            end

            sleep 50.milliseconds unless any_records
          rescue ex
            Log.warn { "Batched fetch error on #{topic}: #{ex.message}" }
            sleep 500.milliseconds
          end
        end
      end
    end

    def close
      @running = false
      @fetcher_mutex.synchronize do
        @active_fetchers.each_key do |part|
          @active_fetchers[part] = false
        end
      end
      @prefetch_channel.close rescue nil
      close_internal
    end

    private def spawn_heartbeat_loop(client : Client, interval_ms : Int32)
      spawn do
        while @running
          sleep (interval_ms / 1000.0).seconds
          break unless @running

          owned_tp = [] of Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions
          group_id = ""
          instance_id = nil
          session_timeout = 30000
          assignor = "uniform"
          member_id = ""
          member_epoch = 0

          @hb_mutex.synchronize do
            if !@assigned_partitions.empty? && !@topic_uuid.empty?
              owned_tp = [
                Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions.new(@topic_uuid, @assigned_partitions),
              ]
            end
            group_id = @config.settings["group.id"]? || "default-group"
            instance_id = @config.settings["group.instance.id"]?
            session_timeout = (@config.settings["session.timeout.ms"]? || "30000").to_i
            assignor = @config.settings["group.remote.assignor"]? || "uniform"
            member_id = @member_id
            member_epoch = @member_epoch
          end

          begin
            r = client.consumer_group_heartbeat(
              group_id: group_id,
              member_id: member_id,
              member_epoch: member_epoch,
              instance_id: instance_id,
              rebalance_timeout_ms: session_timeout,
              subscribed_topic_names: @topics,
              server_assignor: assignor,
              topic_partitions: owned_tp
            )

            if r.error_code == 0
              @hb_mutex.synchronize do
                @member_epoch = r.member_epoch
                apply_assignment(r)
              end
            end
          rescue ex
            Log.error(exception: ex) { "Background heartbeat error" }
          end
        end
      end
    end

    private def close_internal
      @hb_mutex.synchronize do
        return if @closed
        @closed = true
      end

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
          if @using_classic_group_protocol
            client.leave_group(group_id, @member_id) rescue nil
          else
            client.consumer_group_heartbeat(
              group_id: group_id,
              member_id: @member_id,
              member_epoch: -1
            ) rescue nil
          end
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
