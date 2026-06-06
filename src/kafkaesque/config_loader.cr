require "yaml"

module Kafkaesque
  # Helper to load and build Producer/Consumer configurations from a YAML config file and/or environment variables.
  class ConfigLoader
    def self.load_producer_config(file_path : String? = nil) : Producer::Config
      config_hash = load_yaml_and_env(file_path)

      bootstrap_servers = [] of String
      if bs = config_hash[YAML::Any.new("bootstrap_servers")]?
        if bs.raw.is_a?(Array)
          bs.raw.as(Array).each do |server|
            bootstrap_servers << server.to_s
          end
        else
          bootstrap_servers << bs.to_s
        end
      else
        bootstrap_servers = ["localhost:9092"]
      end

      compression_type = config_hash[YAML::Any.new("compression_type")]?.try(&.to_s)

      settings = {} of String => String
      if raw_settings = config_hash[YAML::Any.new("settings")]?
        if raw_settings.raw.is_a?(Hash)
          raw_settings.raw.as(Hash).each do |key, value|
            settings[key.to_s] = value.to_s
          end
        end
      end

      Producer::Config.new(bootstrap_servers: bootstrap_servers, compression_type: compression_type, settings: settings)
    end

    def self.load_consumer_config(file_path : String? = nil) : Consumer::Config
      config_hash = load_yaml_and_env(file_path)

      bootstrap_servers = [] of String
      if bs = config_hash[YAML::Any.new("bootstrap_servers")]?
        if bs.raw.is_a?(Array)
          bs.raw.as(Array).each do |server|
            bootstrap_servers << server.to_s
          end
        else
          bootstrap_servers << bs.to_s
        end
      else
        bootstrap_servers = ["localhost:9092"]
      end

      group_id = config_hash[YAML::Any.new("group_id")]?.try(&.to_s)
      initial_offset_smallest = config_hash[YAML::Any.new("initial_offset_smallest")]?.try(&.to_s) == "true"

      settings = {} of String => String
      if raw_settings = config_hash[YAML::Any.new("settings")]?
        if raw_settings.raw.is_a?(Hash)
          raw_settings.raw.as(Hash).each do |key, value|
            settings[key.to_s] = value.to_s
          end
        end
      end

      Consumer::Config.new(
        bootstrap_servers: bootstrap_servers,
        group_id: group_id,
        initial_offset_smallest: initial_offset_smallest,
        settings: settings
      )
    end

    private def self.load_yaml_and_env(file_path : String?) : Hash(YAML::Any, YAML::Any)
      base_config = {} of YAML::Any => YAML::Any

      # 1. Load from YAML file if exists
      if file_path && File.exists?(file_path)
        begin
          parsed = YAML.parse(File.read(file_path))
          if parsed.raw.is_a?(Hash)
            parsed.raw.as(Hash).each do |k, v|
              base_config[k] = v
            end
          end
        rescue ex
          Log.error(exception: ex) { "Failed to parse YAML config file: #{file_path}" }
        end
      end

      # 2. Layer environment variables on top
      # General settings
      if bs = ENV["KAFKA_BOOTSTRAP_SERVERS"]?
        base_config[YAML::Any.new("bootstrap_servers")] = YAML::Any.new(bs.split(",").map { |s| YAML::Any.new(s.strip) })
      end
      if cmp = ENV["KAFKA_COMPRESSION_TYPE"]?
        base_config[YAML::Any.new("compression_type")] = YAML::Any.new(cmp)
      end
      if gid = ENV["KAFKA_GROUP_ID"]?
        base_config[YAML::Any.new("group_id")] = YAML::Any.new(gid)
      end
      if smallest = ENV["KAFKA_INITIAL_OFFSET_SMALLEST"]?
        base_config[YAML::Any.new("initial_offset_smallest")] = YAML::Any.new(smallest)
      end

      # Setting keys nested under settings
      settings_any = base_config[YAML::Any.new("settings")]?
      settings_hash = if settings_any && settings_any.raw.is_a?(Hash)
                        settings_any.raw.as(Hash)
                      else
                        {} of YAML::Any => YAML::Any
                      end

      ENV.each do |key, value|
        if key.starts_with?("KAFKA_SETTING_")
          # KAFKA_SETTING_ENABLE_IDEMPOTENCE -> "enable.idempotence"
          setting_key = key.sub("KAFKA_SETTING_", "").downcase.gsub('_', '.')
          settings_hash[YAML::Any.new(setting_key)] = YAML::Any.new(value)
        elsif key.starts_with?("KAFKA_SASL_") || key.starts_with?("KAFKA_SSL_")
          # Special mapping for OIDC endpoints/credentials and SSL paths
          # KAFKA_SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL -> "sasl.oauthbearer.token.endpoint.url"
          # KAFKA_SSL_KEYSTORE_LOCATION -> "ssl.keystore.location"
          setting_key = key.sub("KAFKA_", "").downcase.gsub('_', '.')
          settings_hash[YAML::Any.new(setting_key)] = YAML::Any.new(value)
        end
      end

      base_config[YAML::Any.new("settings")] = YAML::Any.new(settings_hash)
      base_config
    end
  end
end
