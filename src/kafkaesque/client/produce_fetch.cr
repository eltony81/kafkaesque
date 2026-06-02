module Kafkaesque
  class Client
    # -----------------------------------------------------------------------
    # Produce: single-message produce with optional idempotency
    # -----------------------------------------------------------------------
    def produce(topic : String, key : String?, value : String?, partition : Int32 = 0, headers : Array(Protocol::RecordHeader) = [] of Protocol::RecordHeader, timestamp : Time? = nil) : Protocol::ProduceResponse
      conn = connection_for_partition(topic, partition)
      
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
      if cb = @on_deliver
        ex = resp.error_code == 0 ? nil : Exception.new("Produce error code: #{resp.error_code}")
        cb.call(resp.topic, resp.partition, resp.base_offset, ex)
      end
      resp
    end

    # -----------------------------------------------------------------------
    # Batching Accumulator: queue records for background dispatch
    # -----------------------------------------------------------------------

    def batch_produce(topic : String, key : String?, value : String?, partition : Int32 = 0, headers : Array(Protocol::RecordHeader) = [] of Protocol::RecordHeader, timestamp : Time? = nil)
      record = Protocol::Record.new(key, value, headers, timestamp: timestamp)
      entry = BatchEntry.new(topic, partition, [record])

      @batch_mutex.synchronize do
        @produced_messages_count += 1
        @produced_bytes_count += value.try(&.bytesize) || 0
        existing = @pending_batch.find { |e| e.topic == topic && e.partition == partition }
        if existing
          existing.records << record
        else
          @pending_batch << entry
        end
      end

      unless @batch_fiber_running
        start_batch_fiber
      end

      total = @batch_mutex.synchronize { @pending_batch.sum(&.records.size) }
      if total >= @batch_max_size
        @batch_channel.send(nil) rescue nil
      end
    end

    def flush_batch : Array(Protocol::ProduceResponse)
      entries = @batch_mutex.synchronize do
        taken = @pending_batch.dup
        @pending_batch.clear
        taken
      end

      responses = [] of Protocol::ProduceResponse
      entries.each do |entry|
        base_seq = -1
        if idempotent?
          slot = "#{entry.topic}:#{entry.partition}"
          base_seq = @sequence_numbers.fetch(slot, 0)
          @sequence_numbers[slot] = base_seq + entry.records.size
        end

        req = Protocol::ProduceRequest.new(
          acks: @acks,
          timeout_ms: 5000_i32,
          topic: entry.topic,
          records: entry.records,
          partition: entry.partition,
          producer_id: @producer_id,
          producer_epoch: @producer_epoch,
          base_sequence: base_seq,
          compression: @compression
        )

        conn = connection_for_partition(entry.topic, entry.partition)
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
        responses << resp
        if cb = @on_deliver
          ex = resp.error_code == 0 ? nil : Exception.new("Produce error code: #{resp.error_code}")
          cb.call(resp.topic, resp.partition, resp.base_offset, ex)
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
      conn = connection_for_partition(topic, partition)
      
      req = Protocol::FetchRequest.new(topic, partition, fetch_offset, min_bytes)
      
      req_io = IO::Memory.new
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
      
      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)
      
      Protocol::ResponseHeader.deserialize(response_dec, flexible: false)
      resp = Protocol::FetchResponse.deserialize(response_dec)
      @consumed_messages_count += resp.records.size
      emit_stats
      resp
    end
  end
end
