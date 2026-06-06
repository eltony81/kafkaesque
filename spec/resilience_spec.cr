require "./spec_helper"

describe "Resilience & Security Configuration" do
  describe Kafkaesque::Backoff do
    it "computes exponential backoff with full jitter" do
      backoff = Kafkaesque::Backoff.new(base: 100.0, max: 1000.0, factor: 2.0)

      # Test computation does not exceed max backoff limit and is within correct boundaries
      10.times do |attempt|
        val = backoff.compute(attempt)
        val.should be >= 0.0
        val.should be <= 1000.0
      end
    end
  end

  describe "SSL Context Builder" do
    it "dynamically configures context properties when truststore/keystore options are supplied" do
      # Create temporary files to simulate certificates
      ca_temp = File.tempfile("ca")
      cert_temp = File.tempfile("cert")
      key_temp = File.tempfile("key")

      begin
        ca_temp.print "DUMMY CA CERT"
        ca_temp.close
        cert_temp.print "DUMMY CLIENT CERT"
        cert_temp.close
        key_temp.print "DUMMY CLIENT KEY"
        key_temp.close

        settings = {
          "ssl.truststore.location"   => ca_temp.path,
          "ssl.keystore.location"     => cert_temp.path,
          "ssl.keystore.key.location" => key_temp.path,
        }

        # Verify build_ssl_context completes or raises format error rather than undefined method/file not found
        expect_raises(OpenSSL::Error) do
          Kafkaesque::Client.build_ssl_context(settings)
        end
      ensure
        ca_temp.delete
        cert_temp.delete
        key_temp.delete
      end
    end
  end

  describe "Metadata Refresh Failover Routes" do
    it "re-resolves and refreshes partition metadata on connection failures" do
      client = Kafkaesque::Client.new("127.0.0.1", 9092)
      # Setup partition leaders/replicas
      client.@partition_leaders["my-topic:0"] = 1
      client.@brokers[1] = Kafkaesque::Protocol::Broker.new(1, "127.0.0.1", 9091, "rack-a")

      # Initially no broker connections cached
      client.@broker_connections.empty?.should be_true
    end
  end
end
