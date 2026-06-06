require "base64"
require "digest"
require "openssl"
require "openssl/hmac"

module Kafkaesque
  module Protocol
    class ScramAuthenticator
      property username : String
      property password : String
      property algorithm : Symbol # :sha1, :sha256 or :sha512
      property client_nonce : String

      def initialize(@username, @password, @algorithm = :sha256)
        # Generate a random 24-character alphanumeric nonce
        @client_nonce = Random::Secure.urlsafe_base64(18).gsub(/[^a-zA-Z0-9]/, "")[0..23]
      end

      def client_first_message : String
        "n,,n=#{username},r=#{@client_nonce}"
      end

      def client_first_message_bare : String
        "n=#{username},r=#{@client_nonce}"
      end

      # Parses server-first-message and computes the client-final-message
      def process_server_first_message(server_first : String) : {String, Bytes}
        parts = {} of String => String
        server_first.split(",").each do |part|
          kv = part.split("=", 2)
          if kv.size == 2
            parts[kv[0]] = kv[1]
          end
        end

        nonce = parts["r"]? || raise "Server first message missing nonce"
        salt_b64 = parts["s"]? || raise "Server first message missing salt"
        iterations_str = parts["i"]? || raise "Server first message missing iterations"
        iterations = iterations_str.to_i

        unless nonce.starts_with?(@client_nonce)
          raise "Server nonce does not match client nonce"
        end

        salt = Base64.decode(salt_b64)

        # 1. PBKDF2 to compute SaltedPassword
        ssl_alg = case @algorithm
                  when :sha1
                    OpenSSL::Algorithm::SHA1
                  when :sha512
                    OpenSSL::Algorithm::SHA512
                  else
                    OpenSSL::Algorithm::SHA256
                  end
        key_size = case @algorithm
                   when :sha1
                     20
                   when :sha512
                     64
                   else
                     32
                   end
        salted_password = OpenSSL::PKCS5.pbkdf2_hmac(@password, salt, iterations, ssl_alg, key_size)

        # 2. ClientKey = HMAC(SaltedPassword, "Client Key")
        client_key = OpenSSL::HMAC.digest(ssl_alg, salted_password, "Client Key".to_slice)

        # 3. StoredKey = SHA(ClientKey)
        stored_key = case @algorithm
                     when :sha1
                       Digest::SHA1.digest(client_key)
                     when :sha512
                       Digest::SHA512.digest(client_key)
                     else
                       Digest::SHA256.digest(client_key)
                     end

        # 4. AuthMessage
        client_final_bare_without_proof = "c=biws,r=#{nonce}"
        auth_message = "#{client_first_message_bare},#{server_first},#{client_final_bare_without_proof}"

        # 5. ClientSignature = HMAC(StoredKey, AuthMessage)
        client_signature = OpenSSL::HMAC.digest(ssl_alg, stored_key, auth_message.to_slice)

        # 6. ClientProof = ClientKey XOR ClientSignature
        client_proof = Bytes.new(client_key.size)
        client_key.size.times do |i|
          client_proof[i] = client_key[i] ^ client_signature[i]
        end

        # 7. ServerKey = HMAC(SaltedPassword, "Server Key")
        server_key = OpenSSL::HMAC.digest(ssl_alg, salted_password, "Server Key".to_slice)

        # 8. ServerSignature = HMAC(ServerKey, AuthMessage)
        server_signature = OpenSSL::HMAC.digest(ssl_alg, server_key, auth_message.to_slice)

        # Format client-final-message
        client_final = "#{client_final_bare_without_proof},p=#{Base64.strict_encode(client_proof)}"

        {client_final, server_signature}
      end

      # Verifies the server-final-message
      def verify_server_final_message(server_final : String, expected_server_signature : Bytes) : Bool
        parts = {} of String => String
        server_final.split(",").each do |part|
          kv = part.split("=", 2)
          if kv.size == 2
            parts[kv[0]] = kv[1]
          end
        end

        sig_b64 = parts["v"]? || raise "Server final message missing signature"
        sig = Base64.decode(sig_b64)

        sig == expected_server_signature
      end
    end
  end
end
