require "../src/kafkaesque"

# 1. Custom Partitioner Implementation
# We can inherit from Partitioner::Base to define custom partition routing algorithms.
class CustomTenantPartitioner < Kafkaesque::Partitioner::Base
  def partition(topic : String, key : Bytes?, value : Bytes?, partitions_count : Int32) : Int32
    return 0 if partitions_count <= 0
    return 0 if key.nil? || key.empty?

    # Route based on a tenant ID prefix in the key (e.g. "tenantA:user1")
    key_str = String.new(key)
    if key_str.starts_with?("tenant-premium")
      # Premium tenant is pinned to partition 0
      0
    else
      # Other keys are distributed among the remaining partitions
      non_premium_count = partitions_count > 1 ? partitions_count - 1 : 1
      h = key_str.hash.abs
      1 + (h % non_premium_count)
    end
  end
end

# 2. Setup config with standard MurmurHash2 partitioner (default)
murmur_config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  settings: {} of String => String
)
# The partitioner defaults to Partitioner::MurmurHash2.new
puts "[INIT] Configured with default MurmurHash2 partitioner: #{murmur_config.partitioner.class}"

# 3. Setup config with our Custom Partitioner
custom_partitioner = CustomTenantPartitioner.new
custom_config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  partitioner: custom_partitioner
)
puts "[INIT] Configured with custom partitioner: #{custom_config.partitioner.class}"

# 4. Instantiate Producer
producer = Kafkaesque::Producer.new(custom_config)

begin
  puts "[PRODUCE] Sending messages..."

  # Message routed dynamically using CustomTenantPartitioner (maps to partition 0)
  producer.produce(
    topic: "tenant-events",
    payload: "Premium action payload",
    key: "tenant-premium:action-1"
  )

  # Message routed dynamically using CustomTenantPartitioner (maps to partition > 0)
  producer.produce(
    topic: "tenant-events",
    payload: "Standard action payload",
    key: "tenant-standard:action-2"
  )

  # Explicitly override partition routing (bypasses partitioner)
  producer.produce(
    topic: "tenant-events",
    payload: "Force routed payload",
    key: "any-key",
    partition: 2
  )

  puts "[SUCCESS] Messages queued for partitioner-driven routing."
ensure
  producer.close
end
