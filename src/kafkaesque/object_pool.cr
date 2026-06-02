module Kafkaesque
  class ObjectPool(T)
    def initialize(&@factory : -> T)
      @pools = Hash(Thread, Deque(T)).new
      @mutex = Mutex.new
    end

    def rent : T
      thread = Thread.current
      pool = @mutex.synchronize do
        @pools[thread] ||= Deque(T).new
      end
      pool.shift? || @factory.call
    end

    def return(obj : T)
      thread = Thread.current
      pool = @mutex.synchronize do
        @pools[thread] ||= Deque(T).new
      end
      pool.push(obj)
    end
  end
end
