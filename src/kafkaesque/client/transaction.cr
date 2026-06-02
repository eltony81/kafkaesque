module Kafkaesque
  class Client
    def add_partitions_to_txn(transactional_id : String, producer_id : Int64, producer_epoch : Int16, topics : Hash(String, Array(Int32))) : Protocol::AddPartitionsToTxnResponse
      conn = @connection || raise "Client is not connected."
      req = Protocol::AddPartitionsToTxnRequest.new(transactional_id, producer_id, producer_epoch, topics)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::AddPartitionsToTxnRequest::API_KEY,
        api_version: Protocol::AddPartitionsToTxnRequest::API_VERSION,
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
      Protocol::AddPartitionsToTxnResponse.deserialize(response_dec)
    end

    def end_txn(transactional_id : String, producer_id : Int64, producer_epoch : Int16, transaction_result : Bool) : Protocol::EndTxnResponse
      conn = @connection || raise "Client is not connected."
      req = Protocol::EndTxnRequest.new(transactional_id, producer_id, producer_epoch, transaction_result)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::EndTxnRequest::API_KEY,
        api_version: Protocol::EndTxnRequest::API_VERSION,
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
      Protocol::EndTxnResponse.deserialize(response_dec)
    end

    def txn_offset_commit(transactional_id : String, group_id : String, producer_id : Int64, producer_epoch : Int16, offsets : Hash(String, Hash(Int32, Int64))) : Protocol::TxnOffsetCommitResponse
      conn = @connection || raise "Client is not connected."
      req = Protocol::TxnOffsetCommitRequest.new(transactional_id, group_id, producer_id, producer_epoch, offsets)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::TxnOffsetCommitRequest::API_KEY,
        api_version: Protocol::TxnOffsetCommitRequest::API_VERSION,
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
      Protocol::TxnOffsetCommitResponse.deserialize(response_dec)
    end
  end
end
