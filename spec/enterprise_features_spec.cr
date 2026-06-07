require "./spec_helper"
require "../src/kafkaesque/mock_broker"
require "file_utils"

class Kafkaesque::Client
  def force_start_ssl_reload_fiber
    start_ssl_reload_fiber
  end
end

describe "Enterprise Observability & Security Features" do
  it "generates correct Prometheus metrics payload format" do
    client = Kafkaesque::Client.new(host: "127.0.0.1", port: 9092)
    metrics = client.prometheus_metrics
    metrics.should contain("kafkaesque_produced_messages_total")
    metrics.should contain("kafkaesque_produced_bytes_total")
    metrics.should contain("kafkaesque_consumed_messages_total")
    metrics.should contain("kafkaesque_broker_connections")
  end

  it "serves Prometheus metrics over embedded HTTP server" do
    broker = Kafkaesque::MockBroker.new
    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: broker.port,
        settings: {
          "metrics.prometheus.port" => "19090",
        }
      )
      client.connect
      sleep 100.milliseconds

      # Hit the HTTP metrics server endpoint
      response = HTTP::Client.get("http://127.0.0.1:19090/metrics")
      response.status_code.should eq(200)
      response.headers["Content-Type"].should eq("text/plain; version=0.0.4")
      response.body.should contain("kafkaesque_broker_connections")
    ensure
      client.try &.close rescue nil
      broker.close
    end
  end

  it "intercepts and mutates records during produce with custom tracing context headers" do
    broker = Kafkaesque::MockBroker.new
    begin
      producer = Kafkaesque::Producer.new(
        Kafkaesque::Producer::Config.new(
          bootstrap_servers: ["127.0.0.1:#{broker.port}"],
          settings: {
            "linger.ms" => "10000",
          }
        )
      )

      # Register tracing send interceptor
      producer.on_send do |record|
        Kafkaesque::Tracing.inject_trace_context(
          headers: record.headers,
          trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
          span_id: "00f067aa0ba902b7",
          sampled: true
        )
        record
      end

      # Produce message
      producer.produce("test-topic", "val", headers: {"other-header" => "val"})

      # Verify that headers now contain injected traceparent
      client = producer.not_nil!.@client.not_nil!
      pending = client.@pending_batch[{"test-topic", 0}]
      pending.size.should eq(1)

      record = pending.first
      ctx = Kafkaesque::Tracing.extract_trace_context(record.headers)
      ctx.should_not be_nil
      if info = ctx
        info[:trace_id].should eq("4bf92f3577b34da6a3ce929d0e0e4736")
        info[:span_id].should eq("00f067aa0ba902b7")
        info[:sampled].should be_true
      end
    ensure
      producer.try &.close rescue nil
      broker.close
    end
  end

  it "reloads SSL/TLS context dynamically when files change" do
    temp_dir = "./scratch/cert_reload_test"
    Dir.mkdir_p(temp_dir)
    cert_file = File.join(temp_dir, "client.crt")
    key_file = File.join(temp_dir, "client.key")

    # Generate real self-signed certs using openssl command line tool to avoid PEM parsing errors
    system("openssl req -x509 -newkey rsa:2048 -keyout #{key_file} -out #{cert_file} -days 1 -nodes -subj \"/CN=localhost\" > /dev/null 2>&1")

    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: 9092,
        settings: {
          "security.protocol"                     => "SSL",
          "ssl.keystore.location"                 => cert_file,
          "ssl.keystore.key.location"             => key_file,
          "ssl.keystore.reload.interval.ms"       => "50",
          "ssl.endpoint.identification.algorithm" => "none",
        }
      )

      # Force start reload fiber without connecting
      client.force_start_ssl_reload_fiber

      initial_ctx = client.@ssl_context
      initial_ctx.should_not be_nil

      # Update files with a valid new PEM certificate
      new_cert_file = File.join(temp_dir, "client_new.crt")
      system("openssl req -x509 -newkey rsa:2048 -keyout #{key_file} -out #{new_cert_file} -days 1 -nodes -subj \"/CN=localhost-new\" > /dev/null 2>&1")
      File.copy(new_cert_file, cert_file)
      sleep 150.milliseconds

      # Verify context was rebuilt (is a new SSL context instance)
      client.@ssl_context.should_not eq(initial_ctx)
    ensure
      client.try &.close rescue nil
      FileUtils.rm_rf(temp_dir) rescue nil
    end
  end

  it "allows registering custom SASL mechanisms and executing them" do
    broker = Kafkaesque::MockBroker.new
    custom_sasl_called = false

    # Register custom sasl mechanism builder
    Kafkaesque::Client.register_sasl_mechanism("MY_CUSTOM_SASL") do |conn, settings|
      custom_sasl_called = true
      settings["custom_val"]?.should eq("hello")
    end

    begin
      client = Kafkaesque::Client.new(
        host: "127.0.0.1",
        port: broker.port,
        settings: {
          "sasl.mechanism" => "MY_CUSTOM_SASL",
          "custom_val"     => "hello",
        }
      )

      client.connect rescue nil
      custom_sasl_called.should be_true
    ensure
      client.try &.close rescue nil
      broker.close
    end
  end
end
