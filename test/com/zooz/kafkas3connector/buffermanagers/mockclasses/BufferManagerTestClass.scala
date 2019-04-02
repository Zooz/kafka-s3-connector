package com.zooz.kafkas3connector.buffermanagers.mockclasses

import play.api.Configuration
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.buffermanagers.BuffersManager
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import com.zooz.kafkas3connector.buffers.Buffer
import akka.actor.SupervisorStrategy
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import scala.language.postfixOps
import Utils.TestBuffer


class BufferManagerTestClass(
  configuration: Configuration,
  metrics:       BufferManagerMetrics)
  extends BuffersManager(configuration, metrics) {
  
  override val expectedExceptions: Set[Class[_ <: Exception]] = Set()

  override def getPartitionKey(kafkaMessage: KafkaMessage): String = {
    s"${kafkaMessage.partition}"
  }

  override def createBuffer(kafkaMessage: KafkaMessage): Buffer = {
    val partitionKey = getPartitionKey(kafkaMessage)
    new TestBuffer(configuration)(partitionKey, kafkaMessage.key.asString)
  }
}