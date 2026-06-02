require "../src/kafkaesque"

# Configure SASL OAuthBearer (OIDC) authentication.
# Ideal for keycloak, okta, or other oauth access token providers.
settings = {
  "sasl.oauthbearer.token.endpoint.url" => "http://localhost:8080/realms/kafka/protocol/openid-connect/token",
  "sasl.oauthbearer.client.id"          => "my-kafka-client",
  "sasl.oauthbearer.client.secret"      => "my-secret-token",
}

# --- OIDC PRODUCER EXAMPLE ---
producer_config = Kafkaesque::Producer::Config.new(
  bootstrap_servers: ["localhost:9093"],
  settings: settings
)

producer = Kafkaesque::Producer.new(producer_config)
begin
  producer.produce("secure-topic", "Securely transmitted OAuth message")
  producer.flush
  puts "[SUCCESS] Message sent securely using OIDC!"
ensure
  producer.close
end

# --- OIDC CONSUMER EXAMPLE ---
consumer_config = Kafkaesque::Consumer::Config.new(
  bootstrap_servers: ["localhost:9093"],
  group_id: "secure-group",
  settings: settings
)

consumer = Kafkaesque::Consumer.new(consumer_config)
consumer.subscribe(["secure-topic"])

begin
  puts "[START] Consumer listening securely..."
  # Streams secure records from the partition
  # consumer.each do |msg|
  #   puts msg.value
  # end
ensure
  consumer.close
end
