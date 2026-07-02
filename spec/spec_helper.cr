require "spec"
require "json"
require "../src/kafkaesque"

# Shared helpers for building MockBroker MetadataResponse (v12, flexible)
# payloads without repeating the compact-array/tag-buffer boilerplate at
# every call site.
def write_mock_metadata_prefix(enc : Kafkaesque::Protocol::Encoder, node_id : Int32, host : String, port : Int32, controller_id : Int32 = node_id)
  enc.write_int32(0) # throttle_time_ms
  enc.write_compact_array([node_id]) do |_|
    enc.write_int32(node_id)
    enc.write_compact_string(host)
    enc.write_int32(port)
    enc.write_compact_string(nil)
    enc.write_tag_buffer
  end
  enc.write_compact_string("mock-cluster")
  enc.write_int32(controller_id)
end

# partitions: array of {partition_index, leader_id}
def write_mock_metadata_topic(enc : Kafkaesque::Protocol::Encoder, name : String, partitions : Array(Tuple(Int32, Int32)), error_code : Int16 = 0_i16, leader_epoch : Int32 = -1)
  enc.write_int16(error_code)
  enc.write_compact_string(name)
  enc.io.write(Bytes.new(16, (name.hash & 0xff).to_u8))
  enc.write_boolean(false)
  enc.write_compact_array(partitions) do |(idx, leader)|
    enc.write_int16(0_i16)
    enc.write_int32(idx)
    enc.write_int32(leader)
    enc.write_int32(leader_epoch)
    enc.write_compact_array([leader]) { |r| enc.write_int32(r) }
    enc.write_compact_array([leader]) { |r| enc.write_int32(r) }
    enc.write_compact_array([] of Int32) { }
    enc.write_tag_buffer
  end
  enc.write_int32(-2147483648)
  enc.write_tag_buffer
end
