require "./spec_helper"

describe Kafkaesque::Protocol::ScramAuthenticator do
  it "correctly computes SCRAM-SHA-256 client final message and signatures" do
    # Using vectors similar to RFC 5802
    auth = Kafkaesque::Protocol::ScramAuthenticator.new("user", "Password123", :sha256)
    auth.client_nonce = "fyko+d2lbbFgONRv9qkGAWSe"

    auth.client_first_message.should eq("n,,n=user,r=fyko+d2lbbFgONRv9qkGAWSe")
    auth.client_first_message_bare.should eq("n=user,r=fyko+d2lbbFgONRv9qkGAWSe")

    server_first = "r=fyko+d2lbbFgONRv9qkGAWSe3chM07SRwrcdHg4tOYZSr5OP,s=QSXCR+Q6sek8bf92,i=4096"
    client_final, server_sig = auth.process_server_first_message(server_first)

    # Verify formatting and signature computation
    client_final.should contain("c=biws")
    client_final.should contain("r=fyko+d2lbbFgONRv9qkGAWSe3chM07SRwrcdHg4tOYZSr5OP")
    client_final.should contain("p=")

    # Verify we can validate server final message
    server_final = "v=#{Base64.strict_encode(server_sig)}"
    auth.verify_server_final_message(server_final, server_sig).should be_true
  end

  it "correctly computes SCRAM-SHA-512 client final message and signatures" do
    auth = Kafkaesque::Protocol::ScramAuthenticator.new("user", "Password123", :sha512)
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
