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

      rec_size = 0_i64
      rec_size += key.is_a?(String) ? key.bytesize : (key.try(&.size) || 0)
      rec_size += value.is_a?(String) ? value.bytesize : (value.try(&.size) || 0)
      headers.each do |h|
        rec_size += h.key.bytesize
        rec_size += h.value.is_a?(String) ? h.value.bytesize : (h.value.try(&.size) || 0)
      end

      start_time = Time.monotonic
      while true
        @batch_mutex.synchronize do
          if @buffer_memory_used + rec_size <= @buffer_memory
            @buffer_memory_used += rec_size
            @produced_messages_count += 1
            @produced_bytes_count += value.is_a?(String) ? value.bytesize : (value.try(&.size) || 0)

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
              @batch_channel.send(nil) rescue nil
            end

            return
          end
        end

        if Time.monotonic - start_time >= @max_block_ms.milliseconds
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

      responses
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
      @batch_channel.send(nil) rescue nil
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
              current_offset += 1
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
      req = Protocol::FetchRequest.new(topic, partition, fetch_offset, min_bytes, max_bytes: @fetch_max_bytes, partition_max_bytes: @max_partition_fetch_bytes)

      retries = @max_retries
      while retries > 0
        begin
          conn = closest_replica_connection_for_partition(topic, partition)

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
  end
end
