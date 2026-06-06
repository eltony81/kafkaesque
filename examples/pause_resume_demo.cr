require "../src/kafkaesque"

# Setup the consumer configuration
config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  group_id: "pause-resume-demo-group",
  settings: {
    "enable.auto.commit" => "false",
  }
)

consumer = Kafkaesque::Consumer.new(config)
consumer.subscribe("device-telemetry")

# Register partition assignment handler to inspect assigned partition IDs
consumer.on_partitions_assigned do |partitions|
  puts "[ASSIGN] Consumer assigned partitions: #{partitions}"
end

puts "[START] Starting Consumer in background fiber..."
spawn do
  consumer.each do |record|
    puts "[RECEIVE] Partition #{record.partition} - Offset #{record.offset} - Value: #{String.new(record.value)}"

    # Simulate backpressure: if partition 0 is processing slowly, pause it
    if record.partition == 0
      puts "[BACKPRESSURE] Pausing partition 0..."
      consumer.pause("device-telemetry", 0)

      # Resume partition 0 after 2 seconds
      spawn do
        sleep 2.seconds
        puts "[RECOVER] Resuming partition 0..."
        consumer.resume("device-telemetry", 0)
      end
    end
  end
end

# Keep main fiber alive for demo
sleep 5.seconds
consumer.close
puts "[STOP] Demo closed."
