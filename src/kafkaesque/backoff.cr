module Kafkaesque
  class Backoff
    property base : Float64
    property max : Float64
    property factor : Float64

    def initialize(@base = 100.0, @max = 10000.0, @factor = 2.0)
    end

    # Calculates sleep duration in milliseconds for the current attempt using Full Jitter
    def compute(attempt : Int32) : Float64
      temp = @base * (@factor ** attempt)
      max_backoff = {temp, @max}.min
      Random.rand(0.0..max_backoff)
    end
  end
end
