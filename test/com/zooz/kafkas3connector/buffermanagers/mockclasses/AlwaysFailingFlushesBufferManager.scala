package com.zooz.kafkas3connector.buffermanagers.mockclasses

import play.api.Configuration
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.buffers.Buffer
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import scala.concurrent.duration.Duration

class AlwaysFailingFlushesBufferManager(
  configuration: Configuration,
  metrics:       BufferManagerMetrics)
  extends BufferManagerTestClass(configuration, metrics) {
  var attempts = 0
  /**
   * A mock class that mimicks a failure to flush a buffer. It will fail as many times
   * as specified in the "test.failing-buffer.failures" configuration
   */
  class AlwaysFailingBuffer(conf: Configuration)(
    fileName: String, newName: String)
    extends Buffer(conf, fileName) {
    def copyBuffer(): Unit = {
      throw new RuntimeException("Mocking an unexpected flush error")
    }
  }

  override def createBuffer(kafkaMessage: KafkaMessage): Buffer = {
    val partitionKey = getPartitionKey(kafkaMessage)
    new AlwaysFailingBuffer(configuration)(partitionKey, kafkaMessage.key.asString)
  }
}
