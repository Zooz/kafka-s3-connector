package com.zooz.kafkas3connector.buffers

import com.zooz.kafkas3connector.kafka.KafkaMessage

/** Adds the ability to generate a buffer key based on a KafkaMessage */
trait Partitioner {
  def getPartitionKey(kafkaMessage: KafkaMessage): String
}
