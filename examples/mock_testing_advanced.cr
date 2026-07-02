# Advanced Kafkaesque::MockBroker usage: overriding a single API key with a
# custom on_request handler while keeping register_topic's stateful defaults
# for everything else, plus latency/drop failure simulation for exercising
# retry and error-handling paths. See examples/mock_testing.cr for the basic
# register_topic-only flow first.

require "../src/kafkaesque"
require "../src/kafkaesque/mock_broker"

puts "=== Custom handler overriding a stateful default ==="
broker = Kafkaesque::MockBroker.new
broker.register_topic("orders", partitions: 1)

fetch_calls = 0
# Fail the first Fetch with NOT_LEADER_OR_FOLLOWER (6), then let it through —
# useful for exercising the client's metadata-refresh-and-retry path.
broker.on_request(1_i16) do |decoder, version|
  fetch_calls += 1
  io = IO::Memory.new
  enc = Kafkaesque::Protocol::Encoder.new(io)
  enc.write_int32(0)     # throttle_time_ms
  enc.write_int16(0_i16) # top-level error_code
  enc.write_int32(0)     # session_id
  enc.write_array(["orders"]) do |topic|
    enc.write_string(topic)
    enc.write_array([0]) do |part|
      enc.write_int32(part)
      enc.write_int16(fetch_calls == 1 ? 6_i16 : 0_i16) # NOT_LEADER_OR_FOLLOWER once, then OK
      enc.write_int64(0_i64)                            # high_watermark
      enc.write_int64(0_i64)                            # last_stable_offset
      enc.write_int64(0_i64)                            # log_start_offset
      enc.write_array([] of Int32) { }
      enc.write_int32(-1) # preferred_read_replica
      enc.write_bytes(Bytes.empty)
    end
  end
  io
end

begin
  client = Kafkaesque::Client.new("127.0.0.1", broker.port)
  client.connect
  client.produce("orders", "k", "first-order") # uses register_topic's default Produce handler

  resp = client.fetch("orders", partition: 0, fetch_offset: 0_i64)
  puts "Fetch settled after #{fetch_calls} attempt(s) with error_code=#{resp.error_code}"
ensure
  client.try(&.close)
  broker.close
end

puts "\n=== Latency & connection-drop simulation ==="
broker2 = Kafkaesque::MockBroker.new
broker2.register_topic("events")
broker2.latency_ms = 150 # every response is delayed this many milliseconds

begin
  client2 = Kafkaesque::Client.new("127.0.0.1", broker2.port)
  client2.connect

  started = Time.instant
  client2.produce("events", "k", "v")
  elapsed = Time.instant - started
  puts "Produce with 150ms simulated latency took #{elapsed.total_milliseconds.round(0)}ms"
ensure
  client2.try(&.close)
  broker2.close
end

puts "\n=== drop_after_requests: broker disappears mid-conversation ==="
broker3 = Kafkaesque::MockBroker.new
broker3.register_topic("events")
broker3.drop_after_requests = 1 # close the socket with no response after the 1st request

begin
  client3 = Kafkaesque::Client.new("127.0.0.1", broker3.port)
  client3.connect
  begin
    client3.produce("events", "k", "v")
    puts "Unexpected: produce did not raise"
  rescue ex
    puts "Produce raised as expected after the broker dropped the connection: #{ex.class}"
  end
ensure
  client3.try(&.close)
  broker3.close
end
