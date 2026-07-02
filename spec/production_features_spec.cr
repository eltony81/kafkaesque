require "./spec_helper"
require "../src/kafkaesque/mock_broker"

describe "Production Ready Enhancements" do
  it "blocks and raises BufferExhaustedException when producer memory buffer is full" do
    broker = Kafkaesque::MockBroker.new
    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: broker.port,
        settings: {
          "buffer.memory" => "10",
          "max.block.ms"  => "100",
          "linger.ms"     => "10000",
        }
      )
      client.connect

      # This should fit within 10 bytes
      client.batch_produce("test-topic", nil, "val1")

      # This second produce pushes it beyond 10 bytes total and should block and raise BufferExhaustedException
      expect_raises(Kafkaesque::BufferExhaustedException) do
        client.batch_produce("test-topic", nil, "val2-too-big-for-small-buffer-memory")
      end
    ensure
      broker.close
    end
  end

  it "unblocks producer when buffer memory is freed by flush" do
    broker = Kafkaesque::MockBroker.new
    broker.on_request(0_i16) do |decoder, version|
      # Mock produce response
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_array(["test-topic"]) do |topic|
        enc.write_string(topic)
        enc.write_array([0]) do |part|
          enc.write_int32(part)
          enc.write_int16(0_i16)
          enc.write_int64(0_i64)
          enc.write_int64(-1_i64)
          enc.write_int64(0_i64)
        end
      end
      enc.write_int32(0)
      io
    end

    broker.on_request(3_i16) do |decoder, version|
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)                        # throttle_time_ms
      enc.write_compact_array([] of String) { } # brokers
      enc.write_compact_string(nil)             # cluster_id
      enc.write_int32(-1)                       # controller_id
      enc.write_compact_array([] of String) { } # topics
      enc.write_tag_buffer
      io
    end

    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: broker.port,
        settings: {
          "buffer.memory" => "50",
          "max.block.ms"  => "1000",
          "linger.ms"     => "10000",
        }
      )
      client.connect

      client.batch_produce("test-topic", nil, "val1-size-40-bytes-12345678901234567890")

      # Spawn a fiber to flush the batch after 50ms, freeing up buffer space
      spawn do
        sleep 50.milliseconds
        client.flush_batch
      end

      # This will block initially but should succeed once flush_batch runs and frees memory
      start_time = Time.instant
      client.batch_produce("test-topic", nil, "val2-size-20-bytes-123")
      ((Time.instant - start_time) >= 30.milliseconds).should be_true
    ensure
      client.try &.close rescue nil
      broker.close
    end
  end

  it "triggers periodic metadata refreshes" do
    broker = Kafkaesque::MockBroker.new
    metadata_call_count = 0

    broker.on_request(3_i16) do |decoder, version|
      metadata_call_count += 1
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int32(0)                        # throttle_time_ms
      enc.write_compact_array([] of String) { } # brokers
      enc.write_compact_string(nil)             # cluster_id
      enc.write_int32(-1)                       # controller_id
      enc.write_compact_array([] of String) { } # topics
      enc.write_tag_buffer
      io
    end

    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: broker.port,
        settings: {
          "topic.metadata.refresh.interval.ms" => "50",
        }
      )
      client.connect

      # Wait for background loops
      sleep 250.milliseconds

      (metadata_call_count > 1).should be_true
    ensure
      client.try &.close rescue nil
      broker.close
    end
  end

  it "periodically refreshes SASL OAuthBearer token in the background" do
    broker = Kafkaesque::MockBroker.new
    # Mock successful OAuthBearer handshake and authentication
    broker.on_request(17_i16) do |decoder, version|
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int16(0_i16) # error_code
      enc.write_array([] of String) { |_| }
      io
    end
    broker.on_request(36_i16) do |decoder, version|
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)
      enc.write_int16(0_i16) # error_code
      enc.write_string("")
      enc.write_int32(0)
      io
    end

    provider_calls = 0
    token_provider = -> {
      provider_calls += 1
      "token-#{provider_calls}"
    }

    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: broker.port,
        oauth_token_provider: token_provider,
        settings: {
          "sasl.mechanism"                             => "OAUTHBEARER",
          "sasl.oauthbearer.token.refresh.interval.ms" => "50",
        }
      )
      client.connect

      sleep 250.milliseconds

      (provider_calls > 2).should be_true
    ensure
      client.try &.close rescue nil
      broker.close
    end
  end
end
