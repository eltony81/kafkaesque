require "http/server"

module Kafkaesque
  class MetricsServer
    @server : HTTP::Server?
    @running = false

    def initialize(@port : Int32, &@metrics_provider : -> String)
    end

    def start
      return if @running
      @running = true

      server = HTTP::Server.new do |context|
        if context.request.path == "/metrics"
          context.response.content_type = "text/plain; version=0.0.4"
          context.response.print @metrics_provider.call
        else
          context.response.status_code = 404
          context.response.print "Not Found"
        end
      end

      @server = server

      spawn do
        begin
          server.listen("0.0.0.0", @port)
        rescue ex
          Log.error { "Metrics HTTP server listen failed: #{ex.message}" }
        ensure
          @running = false
        end
      end
    end

    def close
      @running = false
      if s = @server
        s.close rescue nil
        @server = nil
      end
    end
  end
end
