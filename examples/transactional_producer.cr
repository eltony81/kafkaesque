require "../src/kafkaesque"

# Configure a transactional producer.
# Requires 'transactional.id' and 'enable.idempotence' to enable exactly-once writes.
config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9092"],
  settings: {
    "transactional.id"   => "tx-producer-100",
    "enable.idempotence" => "true",
    "acks"               => "all",
  }
)

producer = Kafkaesque::Producer.new(config)

begin
  # 1. Begin a transactional scope
  puts "🔑 Beginning transaction..."
  producer.begin_transaction

  puts "📤 Writing records atomically to multiple topics..."
  # Produce to topic-a
  producer.produce(
    topic: "topic-a",
    payload: "Atomically written message A",
    key: "tx_key"
  )

  # Produce to topic-b
  producer.produce(
    topic: "topic-b",
    payload: "Atomically written message B",
    key: "tx_key"
  )

  # 2. Commit writes. The broker registers these offset records
  # atomically across partitions. If one fails, none are visible to read_committed consumers.
  puts "💾 Committing transaction..."
  producer.commit_transaction
  puts "✅ Transaction committed successfully!"
rescue ex : Exception
  # 3. Rollback all writes in this transaction scope on error
  puts "❌ Error encountered: #{ex.message}"
  puts "🛑 Aborting transaction..."
  producer.abort_transaction
ensure
  producer.close
end
