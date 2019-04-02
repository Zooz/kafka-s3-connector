package com.zooz.kafkas3connector.kafka

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import play.api.Logger
import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.duration._
import scala.language.postfixOps
import org.apache.kafka.clients.consumer.KafkaConsumer
import Consumer.AssignPartitions
import akka.util.Timeout
import scala.concurrent.Await
import com.zooz.kafkas3connector.buffermanagers.BuffersManager
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import scala.concurrent.Future
import java.nio.ByteBuffer

class KafkaRebalanceListener(
  topic:            String,
  kafkaClient:      KafkaConsumer[ByteBuffer, ByteBuffer],
  parentConsumer:   ActorRef,
  buffersManager:   ActorRef,
  offsetsManager:   ActorRef,
  flushTimeoutSecs: Int)
  extends ConsumerRebalanceListener {
  implicit val flushTimeout = Timeout(flushTimeoutSecs seconds)
  protected val logger = Logger(this.getClass())

  /**
   * Called when partitions are assigned to this consumer. Note - this code will always run
   *  after a call to onPartitionsRevoked was made with all the current partitions handled
   *  by this instance
   *
   *  @param assignedPartitions the list of all partitions assigned to this consumer from this
   *         point
   */
  override def onPartitionsAssigned(
    assignedPartitions: java.util.Collection[TopicPartition]): Unit = {
    if (assignedPartitions.size() > 0) {
      val partitions = assignedPartitions.asScala.map(_.partition()).toList
      logger.info(s"""The Kafka partitions assigned are now: ${partitions.mkString(" ,")}""")

      // Refreshes the statistics for the newly assigned partitions based on the latest
      // committed
      parentConsumer ! AssignPartitions(partitions)
    }
  }

  /**
   * Called when partitions are revoked from this consumer in favor of another consumer.
   *  This method will synchronously trigger a flush of all opened buffer before returning
   *
   *  @param revokedPartitions list of all partitions that were previously handled by this
   *         consumer but now are revoked
   */
  override def onPartitionsRevoked(
    revokedPartitions: java.util.Collection[TopicPartition]): Unit = {
    if (revokedPartitions.size() > 0) {
      val partitions = revokedPartitions.asScala.map(_.partition()).toList
      logger.info(s"""The following Kafka partitions were revoked: ${partitions.mkString(" ,")}""")

      // Asking BuffersManager to flush and waiting for it to finish
      logger.info("Initiating a flush of all buffers")
      val flushCompletedFuture = buffersManager ? BuffersManager.FlushAllBuffers
      Await.ready(flushCompletedFuture, flushTimeoutSecs seconds)

      // Committing to Kafka
      Consumer.commitOffsetsToKafka(topic, offsetsManager, kafkaClient)
    }
  }
}
