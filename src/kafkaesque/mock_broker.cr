require "socket"
require "./protocol/types"
require "./protocol/request"

module Kafkaesque
  class MockBroker
    # "Flexible since" protocol version per API key — the version at which
    # Kafka switched that API to compact/tagged-field ("flexible") encoding.
    # Requests at or above this version use compact strings/arrays and a
    # tagged-field buffer at the end of the header/body; below it, they use
    # the classic fixed-width encoding. Source: the `flexibleVersions` field
    # of each API's schema in the upstream Kafka protocol definitions
    # (clients/src/main/resources/common/message). Only covers API keys this
    # library implements; an API key with no entry here is treated as never
    # flexible, which is correct for every version this client currently
    # sends.
    FLEXIBLE_SINCE = {
       0_i16 => 9_i16,  # Produce
       1_i16 => 12_i16, # Fetch
       2_i16 => 6_i16,  # ListOffsets
       3_i16 => 9_i16,  # Metadata
       8_i16 => 8_i16,  # OffsetCommit
       9_i16 => 6_i16,  # OffsetFetch
      10_i16 => 3_i16,  # FindCoordinator
      11_i16 => 6_i16,  # JoinGroup
      12_i16 => 4_i16,  # SyncGroup
      13_i16 => 4_i16,  # LeaveGroup
      14_i16 => 4_i16,  # Heartbeat
      18_i16 => 3_i16,  # ApiVersions (response header stays non-flexible — special-cased below)
      22_i16 => 2_i16,  # InitProducerId
      24_i16 => 3_i16,  # AddPartitionsToTxn
      25_i16 => 3_i16,  # AddOffsetsToTxn
      26_i16 => 3_i16,  # EndTxn
      28_i16 => 3_i16,  # TxnOffsetCommit
      36_i16 => 2_i16,  # SaslAuthenticate
      68_i16 => 0_i16,  # ConsumerGroupHeartbeat
      71_i16 => 0_i16,  # GetTelemetrySubscriptions
      72_i16 => 0_i16,  # PushTelemetry
      76_i16 => 0_i16,  # ShareGroupHeartbeat
      78_i16 => 0_i16,  # ShareFetch
      79_i16 => 0_i16,  # ShareAcknowledge
    }

    private struct TopicState
      getter topic_id : Bytes
      getter partitions : Int32
      getter leader : Int32

      def initialize(@topic_id, @partitions, @leader)
      end
    end

    getter port : Int32
    @server : TCPServer
    @running = true
    @handlers = {} of Int16 => Proc(Protocol::Decoder, Int16, IO::Memory)

    # Failure & Latency simulation properties
    property latency_ms : Int32 = 0
    property drop_after_requests : Int32? = nil

    # Stateful offset commit tracking
    @committed_offsets = {} of String => Int64
    @offset_mutex = Mutex.new

    # Stateful topic/partition + in-memory log, used by the default
    # Metadata/Produce/Fetch handlers below (see #register_topic). Only
    # engaged as a fallback when a test hasn't registered its own
    # `on_request` handler for that API key.
    @topics = {} of String => TopicState
    @topics_mutex = Mutex.new
    @logs = {} of String => Array(Protocol::Record)
    @log_mutex = Mutex.new

    def initialize
      @server = TCPServer.new("127.0.0.1", 0)
      @port = @server.local_address.port
      spawn_server_loop
    end

    def on_request(api_key : Int16, &block : Protocol::Decoder, Int16 -> IO::Memory)
      @handlers[api_key] = block
    end

    # Registers a topic with `partitions` partitions, all led by `node_id`
    # (default: this mock server itself). Enables the default, stateful
    # Metadata/Produce/Fetch handlers for requests that don't have a custom
    # `on_request` handler registered: Produce appends to an in-memory log
    # per partition and Fetch reads back from it, using the *real*
    # `Protocol::RecordBatch`/`Record` encode/decode the client itself uses —
    # so it's a genuine wire round trip through production serialization
    # code, not a parallel reimplementation, for tests that just need "a
    # topic that behaves like a topic" without hand-encoding responses.
    def register_topic(name : String, partitions : Int32 = 1, node_id : Int32 = 1)
      @topics_mutex.synchronize do
        @topics[name] = TopicState.new(Random::Secure.random_bytes(16), partitions, node_id)
      end
    end

    def close
      @running = false
      @server.close rescue nil
    end

    private def spawn_server_loop
      spawn do
        while @running
          begin
            client_socket = @server.accept
            spawn handle_client(client_socket)
          rescue
            break unless @running
          end
        end
      end
    end

    private def handle_client(socket)
      request_count = 0
      loop do
        break if socket.closed?
        begin
          size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian) rescue nil
          break if size.nil? || size <= 0 || size > 10_000_000

          buf = Bytes.new(size)
          socket.read_fully(buf)

          request_count += 1
          if max_reqs = @drop_after_requests
            if request_count >= max_reqs
              socket.close rescue nil
              break
            end
          end

          mem = IO::Memory.new(buf)
          decoder = Protocol::Decoder.new(mem)

          api_key = decoder.read_int16
          api_version = decoder.read_int16
          correlation_id = decoder.read_int32

          # Check if request has a flexible header
          flexible_request = (min_flexible = FLEXIBLE_SINCE[api_key]?) && api_version >= min_flexible
          flexible_response = flexible_request && (api_key != 18_i16)

          client_id = decoder.read_string
          if flexible_request
            decoder.read_varint # consume RequestHeader tag buffer
          end

          response_body_io = IO::Memory.new
          if handler = @handlers[api_key]?
            body_mem = handler.call(decoder, api_version)
            response_body_io.write(body_mem.to_slice)
          elsif api_key == 8_i16
            # Default stateful OffsetCommit handling
            group_id = decoder.read_compact_string
            generation_id = decoder.read_int32
            member_id = decoder.read_compact_string
            group_instance_id = decoder.read_compact_string

            topic = ""
            partition = 0
            offset = -1_i64

            decoder.read_compact_array do
              topic = decoder.read_compact_string.to_s
              decoder.read_compact_array do
                partition = decoder.read_int32
                offset = decoder.read_int64
                committed_leader_epoch = decoder.read_int32
                metadata = decoder.read_compact_string
                decoder.read_tag_buffer

                @offset_mutex.synchronize do
                  @committed_offsets["#{group_id}:#{topic}:#{partition}"] = offset
                end
              end
              decoder.read_tag_buffer
            end
            decoder.read_tag_buffer

            # Write response
            enc = Protocol::Encoder.new(response_body_io)
            enc.write_int32(0) # throttle_time_ms
            enc.write_compact_array([topic]) do |t|
              enc.write_compact_string(t)
              enc.write_compact_array([partition]) do |p|
                enc.write_int32(p)
                enc.write_int16(0_i16) # error_code
                enc.write_tag_buffer
              end
              enc.write_tag_buffer
            end
            enc.write_tag_buffer
          elsif api_key == 9_i16
            # Default stateful OffsetFetch handling
            group_id = decoder.read_string.to_s
            topic = ""
            partition = 0

            decoder.read_array do
              topic = decoder.read_string.to_s
              decoder.read_array do
                partition = decoder.read_int32
              end
            end

            offset = -1_i64
            @offset_mutex.synchronize do
              offset = @committed_offsets["#{group_id}:#{topic}:#{partition}"]? || -1_i64
            end

            # Write response
            enc = Protocol::Encoder.new(response_body_io)
            enc.write_int32(0) # throttle_time_ms
            enc.write_array([topic]) do |t|
              enc.write_string(t)
              enc.write_array([partition]) do |p|
                enc.write_int32(p)
                enc.write_int64(offset)
                enc.write_string(nil)  # metadata
                enc.write_int16(0_i16) # error_code
              end
            end
          elsif api_key == 18_i16
            # Default ApiVersions response
            enc = Protocol::Encoder.new(response_body_io)
            enc.write_int16(0_i16) # error_code

            # Mock some keys: Produce(0), Fetch(1), ListOffsets(2), Metadata(3), OffsetCommit(8), OffsetFetch(9), ApiVersions(18)
            keys = [0_i16, 1_i16, 2_i16, 3_i16, 8_i16, 9_i16, 18_i16]
            enc.write_compact_array(keys) do |k|
              enc.write_int16(k)
              enc.write_int16(0_i16)
              enc.write_int16(k == 8_i16 ? 9_i16 : (k == 9_i16 ? 3_i16 : 7_i16))
              enc.write_tag_buffer
            end
            enc.write_int32(0) # throttle_time_ms
            enc.write_tag_buffer
          elsif api_key == 3_i16 && !@topics_mutex.synchronize { @topics.empty? }
            handle_default_metadata(decoder, response_body_io)
          elsif api_key == 0_i16 && !@topics_mutex.synchronize { @topics.empty? }
            handle_default_produce(decoder, response_body_io)
          elsif api_key == 1_i16 && !@topics_mutex.synchronize { @topics.empty? }
            handle_default_fetch(decoder, response_body_io)
          else
            # Default response: just error code (0)
            response_body_io.write_bytes(0_i16, IO::ByteFormat::BigEndian)
          end

          # Simulate response latency
          if @latency_ms > 0
            sleep @latency_ms.milliseconds
          end

          resp_mem = IO::Memory.new
          if flexible_response
            resp_size = 4 + 1 + response_body_io.size
            resp_mem.write_bytes(resp_size.to_i32, IO::ByteFormat::BigEndian)
            resp_mem.write_bytes(correlation_id.to_i32, IO::ByteFormat::BigEndian)
            resp_mem.write_byte(0_u8) # ResponseHeader tag buffer
          else
            resp_size = 4 + response_body_io.size
            resp_mem.write_bytes(resp_size.to_i32, IO::ByteFormat::BigEndian)
            resp_mem.write_bytes(correlation_id.to_i32, IO::ByteFormat::BigEndian)
          end
          resp_mem.write(response_body_io.to_slice)

          socket.write(resp_mem.to_slice)
          socket.flush
        rescue ex
          break
        end
      end
    ensure
      socket.close rescue nil
    end

    # Default Metadata (v9-12, flexible/compact) handler backed by
    # #register_topic state. Unregistered topic names come back with
    # UNKNOWN_TOPIC_OR_PARTITION (3), matching real broker behavior when
    # auto-creation is disallowed or fails.
    private def handle_default_metadata(decoder : Protocol::Decoder, response_body_io : IO::Memory)
      requested = decoder.read_compact_array do
        decoder.io.skip(16) # topic_id (unset on request; resolve by name)
        name = decoder.read_compact_string.to_s
        decoder.read_tag_buffer
        name
      end || [] of String
      decoder.read_boolean # allow_auto_topic_creation
      decoder.read_boolean # include_topic_authorized_operations
      decoder.read_tag_buffer

      snapshot = @topics_mutex.synchronize { @topics.dup }
      names = requested.empty? ? snapshot.keys : requested

      enc = Protocol::Encoder.new(response_body_io)
      enc.write_int32(0) # throttle_time_ms
      enc.write_compact_array([1]) do |node_id|
        enc.write_int32(node_id)
        enc.write_compact_string("127.0.0.1")
        enc.write_int32(@port)
        enc.write_compact_string(nil) # rack
        enc.write_tag_buffer
      end
      enc.write_compact_string("mock-cluster")
      enc.write_int32(1) # controller_id
      enc.write_compact_array(names) do |name|
        if state = snapshot[name]?
          enc.write_int16(0_i16) # error_code
          enc.write_compact_string(name)
          enc.io.write(state.topic_id)
          enc.write_boolean(false) # is_internal
          enc.write_compact_array((0...state.partitions).to_a) do |idx|
            enc.write_int16(0_i16) # partition error_code
            enc.write_int32(idx)
            enc.write_int32(state.leader)
            enc.write_int32(-1) # leader_epoch
            enc.write_compact_array([state.leader]) { |r| enc.write_int32(r) }
            enc.write_compact_array([state.leader]) { |r| enc.write_int32(r) }
            enc.write_compact_array([] of Int32) { } # offline_replicas
            enc.write_tag_buffer
          end
          enc.write_int32(-2147483648) # topic_authorized_operations
          enc.write_tag_buffer
        else
          enc.write_int16(3_i16) # UNKNOWN_TOPIC_OR_PARTITION
          enc.write_compact_string(name)
          enc.io.write(Bytes.new(16))
          enc.write_boolean(false)
          enc.write_compact_array([] of Int32) { }
          enc.write_int32(-2147483648)
          enc.write_tag_buffer
        end
      end
      enc.write_tag_buffer
    end

    # Default Produce (v7, non-flexible) handler: appends the decoded records
    # to an in-memory per-partition log and assigns them sequential offsets,
    # exactly like a real broker's log append. Topics/partitions that were
    # never registered via #register_topic come back with
    # UNKNOWN_TOPIC_OR_PARTITION (3) rather than silently accepting the
    # write, matching real broker behavior.
    private def handle_default_produce(decoder : Protocol::Decoder, response_body_io : IO::Memory)
      decoder.read_string # transactional_id
      decoder.read_int16  # acks
      decoder.read_int32  # timeout_ms

      snapshot = @topics_mutex.synchronize { @topics.dup }
      results = {} of String => Array(Tuple(Int32, Int16, Int64))
      decoder.read_array do
        topic = decoder.read_string.to_s
        decoder.read_array do
          partition = decoder.read_int32
          batch_bytes = decoder.read_bytes
          state = snapshot[topic]?

          if state.nil? || partition < 0 || partition >= state.partitions
            (results[topic] ||= [] of Tuple(Int32, Int16, Int64)) << {partition, 3_i16, -1_i64}
          else
            records = batch_bytes ? Protocol::RecordBatch.deserialize_from_bytes(batch_bytes, partition: partition) : [] of Protocol::Record

            base_offset = @log_mutex.synchronize do
              log = (@logs["#{topic}:#{partition}"] ||= [] of Protocol::Record)
              base = log.size.to_i64
              records.each_with_index do |r, i|
                r.offset = base + i
                log << r
              end
              base
            end

            (results[topic] ||= [] of Tuple(Int32, Int16, Int64)) << {partition, 0_i16, base_offset}
          end
        end
      end

      enc = Protocol::Encoder.new(response_body_io)
      enc.write_array(results.keys) do |topic|
        enc.write_string(topic)
        enc.write_array(results[topic]) do |(partition, error_code, base_offset)|
          enc.write_int32(partition)
          enc.write_int16(error_code)
          enc.write_int64(base_offset)
          enc.write_int64(-1_i64) # log_append_time
          enc.write_int64(-1_i64) # log_start_offset
        end
      end
      enc.write_int32(0) # throttle_time_ms
    end

    # Default Fetch (v11, non-flexible) handler: reads back from the same
    # in-memory log #handle_default_produce appends to, re-serialized through
    # the real RecordBatch encoder so it's an authentic wire round trip.
    # Topics/partitions that were never registered via #register_topic come
    # back with UNKNOWN_TOPIC_OR_PARTITION (3), matching real broker behavior.
    private def handle_default_fetch(decoder : Protocol::Decoder, response_body_io : IO::Memory)
      decoder.read_int32 # replica_id
      decoder.read_int32 # max_wait_ms
      decoder.read_int32 # min_bytes
      decoder.read_int32 # max_bytes
      decoder.read_int8  # isolation_level
      decoder.read_int32 # session_id
      decoder.read_int32 # session_epoch

      fetches = [] of Tuple(String, Int32, Int64)
      decoder.read_array do
        topic = decoder.read_string.to_s
        decoder.read_array do
          partition = decoder.read_int32
          decoder.read_int32 # current_leader_epoch
          fetch_offset = decoder.read_int64
          decoder.read_int64 # log_start_offset
          decoder.read_int32 # partition_max_bytes
          fetches << {topic, partition, fetch_offset}
        end
      end
      decoder.read_array { } # forgotten_topics_data
      decoder.read_string    # rack_id

      snapshot = @topics_mutex.synchronize { @topics.dup }

      enc = Protocol::Encoder.new(response_body_io)
      enc.write_int32(0)     # throttle_time_ms
      enc.write_int16(0_i16) # top-level error_code
      enc.write_int32(0)     # session_id

      enc.write_array(fetches) do |(topic, partition, fetch_offset)|
        state = snapshot[topic]?
        unknown = state.nil? || partition < 0 || partition >= state.partitions

        enc.write_string(topic)
        enc.write_array([partition]) do |p|
          if unknown
            enc.write_int32(p)
            enc.write_int16(3_i16)  # error_code: UNKNOWN_TOPIC_OR_PARTITION
            enc.write_int64(-1_i64) # high_watermark
            enc.write_int64(-1_i64) # last_stable_offset
            enc.write_int64(-1_i64) # log_start_offset
            enc.write_array([] of Int32) { }
            enc.write_int32(-1) # preferred_read_replica
            enc.write_bytes(Bytes.empty)
          else
            log = @log_mutex.synchronize { @logs["#{topic}:#{partition}"]?.try(&.dup) } || [] of Protocol::Record
            slice = fetch_offset >= 0 && fetch_offset < log.size ? log[fetch_offset.to_i32..] : [] of Protocol::Record

            enc.write_int32(p)
            enc.write_int16(0_i16)           # error_code
            enc.write_int64(log.size.to_i64) # high_watermark
            enc.write_int64(log.size.to_i64) # last_stable_offset
            enc.write_int64(0_i64)           # log_start_offset
            enc.write_array([] of Int32) { } # aborted_transactions
            enc.write_int32(-1)              # preferred_read_replica
            if slice.empty?
              enc.write_bytes(Bytes.empty)
            else
              batch_io = IO::Memory.new
              Protocol::RecordBatch.new(slice, base_offset: fetch_offset).serialize(batch_io)
              enc.write_bytes(batch_io.to_slice)
            end
          end
        end
      end
    end
  end
end
