module Kafkaesque
  class ObjectPool(T)
    def initialize(&@factory : -> T)
      @pool = Deque(T).new
      @mutex = Mutex.new
    end

    def rent : T
      @mutex.synchronize do
        @pool.shift?
      end || @factory.call
    end

    def return(obj : T)
      @mutex.synchronize do
        @pool.push(obj)
      end
    end
  end
end
