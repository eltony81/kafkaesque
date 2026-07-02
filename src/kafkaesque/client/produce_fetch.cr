module Kafkaesque
  class BufferExhaustedException < Exception
  end

  class Client
    # -----------------------------------------------------------------------
    # Produce: single-message produce with optional idempotency
    # -----------------------------------------------------------------------
    def produce(topic : String, key : Protocol::BytesOrString?, value : Protocol::BytesOrString?, partition : Int32 = 0, headers : Array(Protocol::RecordHeader) = [] of Protocol::RecordHeader, timestamp : Time? = nil) : Protocol::ProduceResponse
      record = Protocol::Record.new(key, value, headers, timestamp: timestamp)

      base_seq = -1
      if idempotent?
        slot = "#{topic}:#{partition}"
        base_seq = @sequence_numbers.fetch(slot, 0)
        @sequence_numbers[slot] = base_seq + 1
      end

      req = Protocol::ProduceRequest.new(
        acks: 1_i16,
        timeout_ms: 5000_i32,
        topic: topic,
        records: [record],
        partition: partition,
        producer_id: @producer_id,
        producer_epoch: @producer_epoch,
        base_sequence: base_seq
      )

      retries = @max_retries
      while retries > 0
        begin
          conn = connection_for_partition(topic, partition)

          req_io = IO::Memory.new
          req_enc = Protocol::Encoder.new(req_io)

          req_header = Protocol::RequestHeader.new(
            api_key: Protocol::ProduceRequest::API_KEY,
            api_version: Protocol::ProduceRequest::API_VERSION,
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
          resp = Protocol::ProduceResponse.deserialize(response_dec)

          if (resp.error_code == 5 || resp.error_code == 6) && retries > 1
            Log.warn { "Leader change/not available for #{topic}:#{partition}. Refreshing metadata and retrying..." }
            refresh_partition_metadata(topic, "#{topic}:#{partition}")
            retries -= 1
            sleep 200.milliseconds
            next
          end

          if cb = @on_deliver
            ex = resp.error_code == 0 ? nil : Exception.new("Produce error code: #{resp.error_code}")
            cb.call(resp.topic, resp.partition, resp.base_offset, ex)
          end
          return resp
        rescue ex : IO::Error
          if retries > 1
            Log.warn { "Network connection error during produce to #{topic}:#{partition}. Refreshing metadata and retrying..." }
            refresh_partition_metadata(topic, "#{topic}:#{partition}")
            retries -= 1
            sleep 200.milliseconds
          else
            raise ex
          end
        end
      end
      raise "Failed to produce record after retries"
    end

    # -----------------------------------------------------------------------
    # Batching Accumulator: queue records for background dispatch
    # -----------------------------------------------------------------------

    def batch_produce(topic : String, key : Protocol::BytesOrString?, value : Protocol::BytesOrString?, partition : Int32 = 0, headers : Array(Protocol::RecordHeader) = [] of Protocol::RecordHeader, timestamp : Time? = nil)
      record = Protocol::Record.new(key, value, headers, timestamp: timestamp)
      batch_produce(topic, record, partition)
    end

    def batch_produce(topic : String, record : Protocol::Record, partition : Int32 = 0)
      rec_size = 0_i64
      k = record.key
      v = record.value
      rec_size += k.is_a?(String) ? k.bytesize : (k.try(&.size) || 0)
      rec_size += v.is_a?(String) ? v.bytesize : (v.try(&.size) || 0)
      record.headers.each do |h|
        h_v = h.value
        rec_size += h.key.bytesize
        rec_size += h_v.is_a?(String) ? h_v.bytesize : (h_v.try(&.size) || 0)
      end

      start_time = Time.instant
      while true
        @batch_mutex.synchronize do
          if @buffer_memory_used + rec_size <= @buffer_memory
            @buffer_memory_used += rec_size
            @produced_messages_count += 1
            @produced_bytes_count += v.is_a?(String) ? v.bytesize : (v.try(&.size) || 0)

            # O(1) hash map lookup
            if records = @pending_batch[{topic, partition}]?
              records << record
            else
              @pending_batch[{topic, partition}] = [record]
            end

            unless @batch_fiber_running
              start_batch_fiber
            end

            total = @pending_batch.sum { |_, recs| recs.size }
            if total >= @batch_max_size
              select
              when @batch_channel.send(nil)
              else
                # if channel is full, do not block main fiber
              end
            end

            return
          end
        end

        if Time.instant - start_time >= @max_block_ms.milliseconds
          raise BufferExhaustedException.new("Failed to allocate memory in batch accumulator within #{@max_block_ms} ms")
        end
        Fiber.yield
        sleep 5.milliseconds
      end
    end

    def flush_batch : Array(Protocol::ProduceResponse)
      entries = @batch_mutex.synchronize do
        taken = @pending_batch.dup
        @pending_batch.clear
        taken
      end

      freed_bytes = 0_i64
      entries.each do |_, records|
        records.each do |rec|
          k = rec.key
          v = rec.value
          freed_bytes += k.is_a?(String) ? k.bytesize : (k.try(&.size) || 0)
          freed_bytes += v.is_a?(String) ? v.bytesize : (v.try(&.size) || 0)
          rec.headers.each do |h|
            h_v = h.value
            freed_bytes += h.key.bytesize
            freed_bytes += h_v.is_a?(String) ? h_v.bytesize : (h_v.try(&.size) || 0)
          end
        end
      end

      @batch_mutex.synchronize do
        val = @buffer_memory_used - freed_bytes
        @buffer_memory_used = val < 0_i64 ? 0_i64 : val
      end

      responses = [] of Protocol::ProduceResponse
      entries.each do |(topic, partition), records|
        base_seq = -1
        if idempotent?
          slot = "#{topic}:#{partition}"
          base_seq = @sequence_numbers.fetch(slot, 0)
          @sequence_numbers[slot] = base_seq + records.size
        end

        req = Protocol::ProduceRequest.new(
          acks: @acks,
          timeout_ms: 5000_i32,
          topic: topic,
          records: records,
          partition: partition,
          producer_id: @producer_id,
          producer_epoch: @producer_epoch,
          base_sequence: base_seq,
          compression: @compression
        )

        retries = @max_retries
        resp = nil
        while retries > 0
          begin
            conn = connection_for_partition(topic, partition)
            req_io = Protocol::BUFFER_POOL.rent
            req_io.clear
            begin
              req_enc = Protocol::Encoder.new(req_io)

              req_header = Protocol::RequestHeader.new(
                api_key: Protocol::ProduceRequest::API_KEY,
                api_version: Protocol::ProduceRequest::API_VERSION,
                correlation_id: next_correlation_id,
                client_id: @client_id,
                flexible: false
              )

              req_header.serialize(req_enc)
              req.serialize(req_enc)

              conn.send_request(req_io.to_slice)
            ensure
              Protocol::BUFFER_POOL.return(req_io)
            end

            response_io = conn.read_response
            response_dec = Protocol::Decoder.new(response_io)
            Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
            resp = Protocol::ProduceResponse.deserialize(response_dec)

            if (resp.error_code == 5 || resp.error_code == 6) && retries > 1
              Log.warn { "Leader change/not available for #{topic}:#{partition} in batch. Refreshing metadata and retrying..." }
              refresh_partition_metadata(topic, "#{topic}:#{partition}")
              retries -= 1
              sleep 200.milliseconds
              next
            end
            break
          rescue ex : IO::Error
            if retries > 1
              Log.warn { "Network connection error during batch produce to #{topic}:#{partition}. Refreshing metadata and retrying..." }
              refresh_partition_metadata(topic, "#{topic}:#{partition}")
              retries -= 1
              sleep 200.milliseconds
            else
              raise ex
            end
          end
        end

        if r = resp
          responses << r
          if cb = @on_deliver
            ex = r.error_code == 0 ? nil : Exception.new("Produce error code: #{r.error_code}")
            cb.call(r.topic, r.partition, r.base_offset, ex)
          end
        end
      end

      emit_stats
      responses
    end

    private def start_batch_fiber
      @batch_fiber_running = true
      spawn do
        loop do
          select
          when @batch_channel.receive
          when timeout(@batch_linger_ms.milliseconds)
          end

          break unless @batch_fiber_running

          has_pending = @batch_mutex.synchronize { !@pending_batch.empty? }
          flush_batch if has_pending
        end
        @batch_fiber_running = false
      end
    end

    def stop_batch_fiber
      @batch_fiber_running = false
      # Non-blocking: `send` on an already-full buffered channel blocks (not an
      # exception, so `rescue` wouldn't help) — e.g. if #close is ever called
      # more than once on the same Client, a second blind `send` here would
      # hang forever since nothing is left to drain the channel.
      select
      when @batch_channel.send(nil)
      else
      end
    end

    # -----------------------------------------------------------------------
    # Consumer Prefetch Queue
    # -----------------------------------------------------------------------

    def start_prefetch(topic : String, partition : Int32 = 0, start_offset : Int64 = 0_i64)
      return if @prefetch_fiber_running
      @prefetch_fiber_running = true
      current_offset = start_offset

      spawn do
        while @prefetch_fiber_running
          begin
            resp = fetch(topic, partition, current_offset)
            resp.records.each do |record|
              break unless @prefetch_fiber_running
              @prefetch_channel.send(record)
            end
            # Derived from the actual last record's offset (rather than
            # blindly incrementing) so a KIP-320 truncation rewind performed
            # inside #fetch is reflected on the next call.
            if last_record = resp.records.last?
              next_offset = last_record.offset + 1
              current_offset = next_offset if next_offset > current_offset
            end
            Fiber.yield if resp.records.empty?
          rescue ex
            break
          end
        end
        @prefetch_fiber_running = false
      end
    end

    def stop_prefetch
      @prefetch_fiber_running = false
    end

    def poll : Protocol::Record?
      @prefetch_channel.receive?
    end

    def poll(timeout : Time::Span) : Protocol::Record?
      select
      when record = @prefetch_channel.receive
        record
      when timeout(timeout)
        nil
      end
    end

    # -----------------------------------------------------------------------
    # Low-level fetch
    # -----------------------------------------------------------------------
    def fetch(topic : String, partition : Int32 = 0, fetch_offset : Int64 = 0_i64, min_bytes : Int32 = 1) : Protocol::FetchResponse
      current_offset = fetch_offset

      retries = @max_retries
      while retries > 0
        begin
          conn = closest_replica_connection_for_partition(topic, partition)
          leader_epoch = leader_epoch_for(topic, partition) || -1
          req = Protocol::FetchRequest.new(topic, partition, current_offset, min_bytes, max_bytes: @fetch_max_bytes, partition_max_bytes: @max_partition_fetch_bytes, rack_id: @client_rack, current_leader_epoch: leader_epoch)

          req_io = Protocol::BUFFER_POOL.rent
          req_io.clear
          begin
            req_enc = Protocol::Encoder.new(req_io)

            req_header = Protocol::RequestHeader.new(
              api_key: Protocol::FetchRequest::API_KEY,
              api_version: Protocol::FetchRequest::API_VERSION,
              correlation_id: next_correlation_id,
              client_id: @client_id,
              flexible: false
            )

            req_header.serialize(req_enc)
            req.serialize(req_enc)

            conn.send_request(req_io.to_slice)
          ensure
            Protocol::BUFFER_POOL.return(req_io)
          end

          response_io = conn.read_response
          response_dec = Protocol::Decoder.new(response_io)

          Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
          resp = Protocol::FetchResponse.deserialize(response_dec)

          if (resp.error_code == 5 || resp.error_code == 6) && retries > 1
            Log.warn { "Leader change/not available for #{topic}:#{partition} in fetch. Refreshing metadata and retrying..." }
            refresh_partition_metadata(topic, "#{topic}:#{partition}")

            # KIP-320: if the leader actually changed epoch (not just a
            # transient blip), check the new leader for truncation before
            # resuming — otherwise a partition that lost uncommitted data in
            # an unclean leader election could silently skip records or hit
            # OFFSET_OUT_OF_RANGE instead of rewinding to the true log end.
            new_epoch = leader_epoch_for(topic, partition)
            if new_epoch && leader_epoch >= 0 && new_epoch != leader_epoch
              begin
                ole = offset_for_leader_epoch(topic, partition, new_epoch, leader_epoch)
                if ole.error_code == 0 && ole.end_offset >= 0 && ole.end_offset < current_offset
                  Log.warn { "Detected log truncation on #{topic}:#{partition}: rewinding from #{current_offset} to #{ole.end_offset} (epoch #{leader_epoch} -> #{new_epoch})" }
                  current_offset = ole.end_offset
                end
              rescue ole_ex
                Log.debug { "OffsetForLeaderEpoch check failed for #{topic}:#{partition}: #{ole_ex.message}" }
              end
            end

            retries -= 1
            sleep 200.milliseconds
            next
          end

          @consumed_messages_count += resp.records.size
          emit_stats
          return resp
        rescue ex : IO::Error
          if retries > 1
            Log.warn { "Network connection error during fetch from #{topic}:#{partition}. Refreshing metadata and retrying..." }
            refresh_partition_metadata(topic, "#{topic}:#{partition}")
            retries -= 1
            sleep 200.milliseconds
          else
            raise ex
          end
        end
      end
      raise "Failed to fetch record after retries"
    end

    # -----------------------------------------------------------------------
    # KIP-227 incremental fetch sessions: fetch every given partition of one
    # topic in a single request per broker (grouped by
    # closest_replica_connection_for_partition), instead of one FetchRequest
    # per partition. Each broker `Connection` tracks its own session state
    # (see Connection#fetch_session_*), so repeated calls with the same
    # partition set only need to send changed fetch offsets — the broker
    # remembers the rest.
    #
    # Best-effort: a group of partitions that fails (network error, leader
    # change) is dropped from the result for this call and its metadata is
    # refreshed for the next one, rather than retried inline — callers
    # (Consumer's per-tick fetch loop) already retry on the next poll.
    # -----------------------------------------------------------------------
    def fetch_many(topic : String, offsets : Hash(Int32, Int64), min_bytes : Int32 = 1) : Hash(Int32, Protocol::FetchSessionPartitionResult)
      return {} of Int32 => Protocol::FetchSessionPartitionResult if offsets.empty?

      by_connection = Hash(Connection, Array(Int32)).new { |h, k| h[k] = [] of Int32 }
      offsets.each_key do |partition|
        conn = closest_replica_connection_for_partition(topic, partition)
        by_connection[conn] << partition
      end

      results = {} of Int32 => Protocol::FetchSessionPartitionResult
      by_connection.each do |conn, parts|
        begin
          fetch_many_on_connection(conn, topic, parts, offsets, min_bytes).each { |k, v| results[k] = v }
        rescue ex
          Log.warn { "fetch_many error on #{topic} partitions #{parts}: #{ex.message}. Refreshing metadata for the next attempt." }
          parts.each { |p| refresh_partition_metadata(topic, "#{topic}:#{p}") }
        end
      end

      @consumed_messages_count += results.values.sum { |r| r.records.size }
      emit_stats
      results
    end

    private def fetch_many_on_connection(conn : Connection, topic : String, requested_partitions : Array(Int32), offsets : Hash(Int32, Int64), min_bytes : Int32) : Hash(Int32, Protocol::FetchSessionPartitionResult)
      if conn.fetch_session_topic != topic
        conn.fetch_session_id = 0
        conn.fetch_session_epoch = -1
        conn.fetch_session_partitions.clear
        conn.fetch_session_topic = topic
      end

      forgotten = conn.fetch_session_partitions.to_a - requested_partitions
      session_epoch = conn.fetch_session_epoch == -1 ? 0 : conn.fetch_session_epoch + 1

      specs = requested_partitions.map do |p|
        epoch = leader_epoch_for(topic, p) || -1
        Protocol::FetchPartitionSpec.new(p, offsets[p], epoch)
      end

      req = Protocol::FetchSessionRequest.new(
        topic, specs,
        session_id: conn.fetch_session_id,
        session_epoch: session_epoch,
        forgotten_partitions: forgotten,
        max_bytes: @fetch_max_bytes,
        partition_max_bytes: @max_partition_fetch_bytes,
        rack_id: @client_rack
      )

      req_io = Protocol::BUFFER_POOL.rent
      req_io.clear
      begin
        req_enc = Protocol::Encoder.new(req_io)

        req_header = Protocol::RequestHeader.new(
          api_key: Protocol::FetchSessionRequest::API_KEY,
          api_version: Protocol::FetchSessionRequest::API_VERSION,
          correlation_id: next_correlation_id,
          client_id: @client_id,
          flexible: false
        )

        req_header.serialize(req_enc)
        req.serialize(req_enc)

        conn.send_request(req_io.to_slice)
      ensure
        Protocol::BUFFER_POOL.return(req_io)
      end

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)
      Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
      resp = Protocol::FetchSessionResponse.deserialize(response_dec)

      if resp.error_code != 0
        # Session-level error (e.g. FETCH_SESSION_ID_NOT_FOUND,
        # INVALID_FETCH_SESSION_EPOCH) — reset so the next call opens fresh.
        conn.fetch_session_id = 0
        conn.fetch_session_epoch = -1
        conn.fetch_session_partitions.clear
        raise "Fetch session error: #{resp.error_code}"
      end

      conn.fetch_session_id = resp.session_id
      conn.fetch_session_epoch = session_epoch
      conn.fetch_session_partitions.concat(requested_partitions)
      forgotten.each { |p| conn.fetch_session_partitions.delete(p) }

      resp.partitions
    end
  end
end
