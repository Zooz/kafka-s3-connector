package com.zooz.kafkas3connector.buffermanagers.mockclasses

import play.api.Configuration
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.buffers.Buffer
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import play.api.Logger
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import scala.concurrent.duration.Duration

class PartlyFailingFlushesBufferManager(
  configuration: Configuration,
  metrics:       BufferManagerMetrics)
  extends BufferManagerTestClass(configuration, metrics) {
  var attempts = 0
  protected val logger = Logger(this.getClass())
  /**
   * A mock class that mimicks a failure to flush a buffer. It will fail as many times
   * as specified in the "test.failing-buffer.failures" configuration
   */
  class PartlyFailingBuffer(conf: Configuration)(
    fileName: String, newName: String)
    extends Buffer(conf, fileName) {
    def copyBuffer(): Unit = {
      val failedAttempts = conf.get[Int]("test.failing-buffer.failures")
      if (attempts < failedAttempts) {
        attempts += 1
        Thread.sleep(500)
        throw new TestRestartableException(
            s"Mimicking a flush failure! Attempt: $attempts / $failedAttempts")
      } else {
        logger.info(s"Attempt $attempts to flush has succeeded")
      }
    }
  }

  override def createBuffer(kafkaMessage: KafkaMessage): Buffer = {
    val partitionKey = getPartitionKey(kafkaMessage)
    new PartlyFailingBuffer(configuration)(partitionKey, kafkaMessage.key.asString)
  }
  
  /**
   * Allowing restarts of the BufferFlushers for known exceptions
   */
  override val expectedExceptions: Set[Class[_ <: Exception]] = 
    Set(classOf[TestRestartableException])
}
