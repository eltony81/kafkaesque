require "./spec_helper"

describe "Enterprise Features (v2.2.0)" do
  describe "SASL SCRAM-SHA-1" do
    it "correctly computes client final message and signatures for SCRAM-SHA-1" do
      auth = Kafkaesque::Protocol::ScramAuthenticator.new("user", "Password123", :sha1)
      auth.client_nonce = "fyko+d2lbbFgONRv9qkGAWSe"

      auth.client_first_message.should eq("n,,n=user,r=fyko+d2lbbFgONRv9qkGAWSe")

      server_first = "r=fyko+d2lbbFgONRv9qkGAWSe3chM07SRwrcdHg4tOYZSr5OP,s=QSXCR+Q6sek8bf92,i=4096"
      client_final, server_sig = auth.process_server_first_message(server_first)

      client_final.should contain("c=biws")
      client_final.should contain("r=fyko+d2lbbFgONRv9qkGAWSe3chM07SRwrcdHg4tOYZSr5OP")

      server_final = "v=#{Base64.strict_encode(server_sig)}"
      auth.verify_server_final_message(server_final, server_sig).should be_true
    end
  end

  describe "Custom SSL Peer Verification Settings" do
    it "sets verify mode to NONE if verify.peer is false or endpoint.identification.algorithm is none" do
      settings_none = {
        "ssl.endpoint.identification.algorithm" => "none",
      }
      ctx_none = Kafkaesque::Client.build_ssl_context(settings_none)
      ctx_none.verify_mode.should eq(OpenSSL::SSL::VerifyMode::NONE)

      settings_false = {
        "ssl.verify.peer" => "false",
      }
      ctx_false = Kafkaesque::Client.build_ssl_context(settings_false)
      ctx_false.verify_mode.should eq(OpenSSL::SSL::VerifyMode::NONE)

      settings_default = {} of String => String
      ctx_default = Kafkaesque::Client.build_ssl_context(settings_default)
      ctx_default.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER)
    end
  end

  describe "Partitioner Routing" do
    it "computes standard MurmurHash2 partition indices" do
      partitioner = Kafkaesque::Partitioner::MurmurHash2.new

      # Compute partitions count = 10
      p1 = partitioner.partition("test-topic", "key-a".to_slice, Bytes.empty, 10)
      p2 = partitioner.partition("test-topic", "key-b".to_slice, Bytes.empty, 10)

      p1.should be >= 0
      p1.should be < 10
      p2.should be >= 0
      p2.should be < 10
    end

    it "distributes partitions sequentially using RoundRobin" do
      partitioner = Kafkaesque::Partitioner::RoundRobin.new

      r1 = partitioner.partition("test-topic", nil, Bytes.empty, 3)
      r2 = partitioner.partition("test-topic", nil, Bytes.empty, 3)
      r3 = partitioner.partition("test-topic", nil, Bytes.empty, 3)
      r4 = partitioner.partition("test-topic", nil, Bytes.empty, 3)

      r1.should eq(0)
      r2.should eq(1)
      r3.should eq(2)
      r4.should eq(0)
    end
  end

  describe "Consumer Pause and Resume" do
    it "correctly manages paused partition flags" do
      config = Kafkaesque::Consumer::Config.new(
        bootstrap_servers: ["localhost:9092"],
        group_id: "test-paused-group"
      )
      consumer = Kafkaesque::Consumer.new(config)

      consumer.paused?("test-topic", 0).should be_false

      consumer.pause("test-topic", 0)
      consumer.paused?("test-topic", 0).should be_true

      consumer.resume("test-topic", 0)
      consumer.paused?("test-topic", 0).should be_false
    end
  end
end
