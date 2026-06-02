module Kafkaesque
  class Client
    TIMESTAMP_EARLIEST = -2_i64
    TIMESTAMP_LATEST   = -1_i64

    def offset_commit(group_id : String, generation_id : Int32, member_id : String,
                      topic : String, partition : Int32, offset : Int64) : Protocol::OffsetCommitResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::OffsetCommitRequest.new(group_id, generation_id, member_id, topic, partition, offset)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::OffsetCommitRequest::API_KEY,
        api_version: Protocol::OffsetCommitRequest::API_VERSION,
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
      Protocol::OffsetCommitResponse.deserialize(response_dec)
    end

    def offset_fetch(group_id : String, topic : String, partition : Int32) : Protocol::OffsetFetchResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::OffsetFetchRequest.new(group_id, topic, partition)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::OffsetFetchRequest::API_KEY,
        api_version: Protocol::OffsetFetchRequest::API_VERSION,
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
      Protocol::OffsetFetchResponse.deserialize(response_dec)
    end

    # Query the earliest or latest offset for a topic partition.
    # Use TIMESTAMP_EARLIEST (-2) or TIMESTAMP_LATEST (-1).
    def list_offsets(topic : String, partition : Int32, timestamp : Int64 = TIMESTAMP_EARLIEST) : Protocol::ListOffsetsResponse
      conn = connection_for_partition(topic, partition)

      req = Protocol::ListOffsetsRequest.new(topic, partition, timestamp)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::ListOffsetsRequest::API_KEY,
        api_version: Protocol::ListOffsetsRequest::API_VERSION,
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
      Protocol::ListOffsetsResponse.deserialize(response_dec)
    end
  end
end
