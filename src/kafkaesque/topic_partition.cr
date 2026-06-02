module Kafkaesque
  struct TopicPartition
    getter topic : String
    getter partition : Int32

    def initialize(@topic : String, @partition : Int32)
    end
  end
end
