module Kafkaesque
  module Tracing
    # Helper to inject W3C trace context into record headers
    def self.inject_trace_context(headers : Array(Protocol::RecordHeader), trace_id : String, span_id : String, sampled : Bool = true)
      flags = sampled ? "01" : "00"
      traceparent = "00-#{trace_id}-#{span_id}-#{flags}"

      # Remove any existing traceparent
      headers.reject! { |h| h.key == "traceparent" }
      headers << Protocol::RecordHeader.new("traceparent", traceparent)
    end

    # Helper to extract trace context from record headers
    def self.extract_trace_context(headers : Array(Protocol::RecordHeader)) : NamedTuple(trace_id: String, span_id: String, sampled: Bool)?
      h = headers.find { |hdr| hdr.key == "traceparent" }
      return nil unless h

      val = h.value
      return nil unless val.is_a?(String)

      parts = val.split("-")
      return nil if parts.size < 4

      {
        trace_id: parts[1],
        span_id:  parts[2],
        sampled:  parts[3] == "01",
      }
    end
  end
end
