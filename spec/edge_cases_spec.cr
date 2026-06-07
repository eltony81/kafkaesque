require "./spec_helper"
require "../src/kafkaesque/mock_broker"

describe "Protocol Edge Cases & Boundary Inputs" do
  describe Kafkaesque::Protocol::Decoder do
    it "raises exception for excessively long varints" do
      # 10 bytes with MSB set (0x80) represents an invalid, too long varint
      io = IO::Memory.new(Bytes.new(12, 0x80_u8))
      dec = Kafkaesque::Protocol::Decoder.new(io)
      expect_raises(Exception, "Varint too long") do
        dec.read_varlong
      end
    end

    it "raises exception for excessively long uvarints" do
      io = IO::Memory.new(Bytes.new(12, 0x80_u8))
      dec = Kafkaesque::Protocol::Decoder.new(io)
      expect_raises(Exception, "Uvarint too long") do
        dec.read_uvarint64
      end
    end

    it "raises EOFError on truncated inputs for standard types" do
      io = IO::Memory.new(Bytes.empty)
      dec = Kafkaesque::Protocol::Decoder.new(io)
      expect_raises(IO::EOFError) do
        dec.read_int8
      end

      io2 = IO::Memory.new(Slice[0x01_u8])
      dec2 = Kafkaesque::Protocol::Decoder.new(io2)
      expect_raises(IO::EOFError) do
        dec2.read_int16
      end
    end

    it "raises exception on negative string and bytes length below -1" do
      # Let's mock a length of -5 (invalid)
      io = IO::Memory.new
      io.write_bytes(-5_i16, IO::ByteFormat::BigEndian)
      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      expect_raises(Exception, "Negative string length: -5") do
        dec.read_string
      end

      io2 = IO::Memory.new
      io2.write_bytes(-5_i32, IO::ByteFormat::BigEndian)
      io2.rewind
      dec2 = Kafkaesque::Protocol::Decoder.new(io2)
      expect_raises(Exception, "Negative bytes length: -5") do
        dec2.read_bytes
      end
    end

    it "correctly decodes empty/null/compact strings and bytes" do
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      enc.write_compact_string(nil)
      enc.write_compact_string("")
      enc.write_compact_bytes(nil)
      enc.write_compact_bytes(Bytes.empty)

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      dec.read_compact_string.should be_nil
      dec.read_compact_string.should eq("")
      dec.read_compact_bytes.should be_nil
      dec.read_compact_bytes.should eq(Bytes.empty)
    end

    it "handles skipping tag buffers with tagged fields" do
      io = IO::Memory.new
      enc = Kafkaesque::Protocol::Encoder.new(io)

      # Write tag count = 2
      enc.write_uvarint(2)

      # Tag 1, length 3, content [1, 2, 3]
      enc.write_uvarint(1)
      enc.write_uvarint(3)
      io.write(Slice[1_u8, 2_u8, 3_u8])

      # Tag 2, length 0
      enc.write_uvarint(2)
      enc.write_uvarint(0)

      io.rewind
      dec = Kafkaesque::Protocol::Decoder.new(io)
      # Should successfully parse/skip both tagged fields
      dec.read_tag_buffer
      dec.io.pos.should eq(io.size)
    end
  end

  describe Kafkaesque::MockBroker do
    it "simulates latency correctly" do
      broker = Kafkaesque::MockBroker.new
      broker.latency_ms = 150

      begin
        client = Kafkaesque::Client.new("127.0.0.1", broker.port)
        start_time = Time.monotonic
        client.connect
        duration = Time.monotonic - start_time
        duration.to_f.should be >= 0.14
      ensure
        broker.close
      end
    end

    it "drops socket connection after configured number of requests" do
      broker = Kafkaesque::MockBroker.new
      broker.drop_after_requests = 1 # Drops right after the ApiVersions handshake (which is the first request during connect)

      begin
        client = Kafkaesque::Client.new("127.0.0.1", broker.port)
        # The first request (ApiVersions) during connect might succeed or be cut off,
        # but subsequent ones will fail because the socket is closed.
        expect_raises(Exception) do
          client.connect
          # Attempt a second request which must fail
          client.get_telemetry_subscription
        end
      ensure
        broker.close
      end
    end

    it "performs stateful offset commits and offset fetches" do
      broker = Kafkaesque::MockBroker.new
      socket = nil

      begin
        socket = TCPSocket.new("127.0.0.1", broker.port)

        # Test commit
        # (Generation 0, member_id "member", metadata "meta")
        commit_req = Kafkaesque::Protocol::OffsetCommitRequest.new(
          "test-group", 0, "member", "test-topic", 0, 1024_i64, "meta"
        )

        # Build envelope and send
        io = IO::Memory.new
        enc = Kafkaesque::Protocol::Encoder.new(io)
        # Request Header: api_key(8), api_version(9), corr_id(1), client_id
        enc.write_int16(8_i16)
        enc.write_int16(9_i16)
        enc.write_int32(1)
        enc.write_string("client")
        enc.write_varint(0) # request tag buffer

        commit_req.serialize(enc)

        # Send raw request
        payload_io = IO::Memory.new
        payload_io.write_bytes(io.size.to_i32, IO::ByteFormat::BigEndian)
        payload_io.write(io.to_slice)
        socket.write(payload_io.to_slice)
        socket.flush

        # Read response size and correlation_id
        resp_size = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        resp_corr = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        resp_tag_buf = socket.read_byte # response flexible header tag buffer

        resp_buf = Bytes.new(resp_size - 5)
        socket.read_fully(resp_buf)
        resp_dec = Kafkaesque::Protocol::Decoder.new(IO::Memory.new(resp_buf))
        resp = Kafkaesque::Protocol::OffsetCommitResponse.deserialize(resp_dec)
        resp.error_code.should eq(0)

        # Test fetch
        fetch_req = Kafkaesque::Protocol::OffsetFetchRequest.new("test-group", "test-topic", 0)
        io2 = IO::Memory.new
        enc2 = Kafkaesque::Protocol::Encoder.new(io2)
        # Request Header: api_key(9), api_version(3), corr_id(2), client_id
        enc2.write_int16(9_i16)
        enc2.write_int16(3_i16)
        enc2.write_int32(2)
        enc2.write_string("client")

        fetch_req.serialize(enc2)

        payload_io2 = IO::Memory.new
        payload_io2.write_bytes(io2.size.to_i32, IO::ByteFormat::BigEndian)
        payload_io2.write(io2.to_slice)
        socket.write(payload_io2.to_slice)
        socket.flush

        resp_size2 = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        resp_corr2 = socket.read_bytes(Int32, IO::ByteFormat::BigEndian)
        resp_buf2 = Bytes.new(resp_size2 - 4)
        socket.read_fully(resp_buf2)
        resp_dec2 = Kafkaesque::Protocol::Decoder.new(IO::Memory.new(resp_buf2))
        resp2 = Kafkaesque::Protocol::OffsetFetchResponse.deserialize(resp_dec2)

        resp2.committed_offset.should eq(1024_i64)
      ensure
        socket.try &.close rescue nil
        broker.close
      end
    end
  end
end
