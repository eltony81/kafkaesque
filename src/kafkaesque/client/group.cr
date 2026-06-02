module Kafkaesque
  class Client
    def find_coordinator(group_id : String) : Protocol::FindCoordinatorResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::FindCoordinatorRequest.new(group_id)
      
      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::FindCoordinatorRequest::API_KEY,
        api_version: Protocol::FindCoordinatorRequest::API_VERSION,
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
      Protocol::FindCoordinatorResponse.deserialize(response_dec)
    end

    def join_group(group_id : String, member_id : String) : Protocol::JoinGroupResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::JoinGroupRequest.new(group_id, member_id)
      
      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::JoinGroupRequest::API_KEY,
        api_version: Protocol::JoinGroupRequest::API_VERSION,
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
      Protocol::JoinGroupResponse.deserialize(response_dec)
    end

    def sync_group(group_id : String, generation_id : Int32, member_id : String) : Protocol::SyncGroupResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::SyncGroupRequest.new(group_id, generation_id, member_id)
      
      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::SyncGroupRequest::API_KEY,
        api_version: Protocol::SyncGroupRequest::API_VERSION,
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
      Protocol::SyncGroupResponse.deserialize(response_dec)
    end

    def heartbeat(group_id : String, generation_id : Int32, member_id : String) : Protocol::HeartbeatResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::HeartbeatRequest.new(group_id, generation_id, member_id)
      
      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)
      
      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::HeartbeatRequest::API_KEY,
        api_version: Protocol::HeartbeatRequest::API_VERSION,
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
      Protocol::HeartbeatResponse.deserialize(response_dec)
    end

    def spawn_heartbeat_fiber(group_id : String, member_id : String, generation_id : Int32)
      return if @heartbeat_fiber_running
      @heartbeat_fiber_running = true
      spawn do
        while @heartbeat_fiber_running
          sleep @heartbeat_interval_ms.milliseconds
          break unless @heartbeat_fiber_running
          begin
            heartbeat(group_id, generation_id, member_id)
          rescue ex
            break
          end
        end
      end
    end

    def stop_heartbeat_fiber
      @heartbeat_fiber_running = false
    end

    def leave_group(group_id : String, member_id : String) : Protocol::LeaveGroupResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::LeaveGroupRequest.new(group_id, member_id)

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::LeaveGroupRequest::API_KEY,
        api_version: Protocol::LeaveGroupRequest::API_VERSION,
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
      Protocol::LeaveGroupResponse.deserialize(response_dec)
    end

    def consumer_group_heartbeat(
      group_id : String,
      member_id : String,
      member_epoch : Int32,
      instance_id : String? = nil,
      rack_id : String? = nil,
      rebalance_timeout_ms : Int32 = 30000,
      subscribed_topic_names : Array(String)? = nil,
      subscribed_topic_regex : String? = nil,
      server_assignor : String? = nil,
      topic_partitions : Array(Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions) = [] of Protocol::ConsumerGroupHeartbeatRequest::TopicPartitions
    ) : Protocol::ConsumerGroupHeartbeatResponse
      conn = @connection || raise "Client is not connected. Call #connect first."
      req = Protocol::ConsumerGroupHeartbeatRequest.new(
        group_id: group_id,
        member_id: member_id,
        member_epoch: member_epoch,
        instance_id: instance_id,
        rack_id: rack_id,
        rebalance_timeout_ms: rebalance_timeout_ms,
        subscribed_topic_names: subscribed_topic_names,
        subscribed_topic_regex: subscribed_topic_regex,
        server_assignor: server_assignor,
        topic_partitions: topic_partitions
      )

      req_io = IO::Memory.new
      req_enc = Protocol::Encoder.new(req_io)

      req_header = Protocol::RequestHeader.new(
        api_key: Protocol::ConsumerGroupHeartbeatRequest::API_KEY,
        api_version: Protocol::ConsumerGroupHeartbeatRequest::API_VERSION,
        correlation_id: next_correlation_id,
        client_id: @client_id,
        flexible: true
      )

      req_header.serialize(req_enc)
      req.serialize(req_enc)

      conn.send_request(req_io.to_slice)

      response_io = conn.read_response
      response_dec = Protocol::Decoder.new(response_io)
      Protocol::ResponseHeader.deserialize(response_dec, flexible: true)
      Protocol::ConsumerGroupHeartbeatResponse.deserialize(response_dec)
    end
  end
end
