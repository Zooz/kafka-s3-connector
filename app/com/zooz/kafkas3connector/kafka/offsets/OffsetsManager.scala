package com.zooz.kafkas3connector.kafka.offsets

import org.apache.kafka.common.{ PartitionInfo, TopicPartition }
import org.apache.kafka.clients.consumer.KafkaConsumer
import play.api.Logger
import com.zooz.kafkas3connector.kafka.Consumer._
import com.zooz.kafkas3connector.kafka.MessageOffset
import java.nio.ByteBuffer
import akka.actor.ActorLogging
import javax.inject.Singleton
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.language.postfixOps
import play.api.Configuration
import javax.inject.Inject
import com.zooz.kafkas3connector.kafka.OffsetsStartPoint
import com.zooz.kafkas3connector.kafka.Helpers
import scala.concurrent.duration.FiniteDuration

/**
 * A case class describing the read and latest offset of a partition. Used to answer the
 * ReportPartitionsStatus request
 */
@Singleton
case class PartitionStatus(
  partition:  MessageOffset.Partition,
  offsetRead: MessageOffset.Offset,
  lastOffset: MessageOffset.Offset,
  lag:        MessageOffset.Offset)

/**
 * An actor used to keep track of the offsets read by the Consumer actor. Also reports their lag
 *  from the topic's last offsets
 *
 * @param conf: Play's configuration
 */
class OffsetsManager @Inject() (conf: Configuration)
  extends Actor with ActorLogging {
  import OffsetsManager._

  val topic: String = conf.get[String]("kafka.topic")
  val kafkaStartPoint = OffsetsStartPoint.withName(conf.get[String]("kafka.default_startpoint"))

  private val metadataConsumer: KafkaConsumer[ByteBuffer, ByteBuffer] =
    Helpers.createKafkaConsumer(conf)

  val offsetsTracker = new OffsetsTracker(metadataConsumer, topic, kafkaStartPoint)

  override def receive: Receive = {
    trackOffsets(Map[MessageOffset.Partition, MessageOffset.Offset]())
  }

  def trackOffsets(currentOffsets: Map[MessageOffset.Partition, MessageOffset.Offset]): Receive = {
    case initOffsets: InitOffsets =>
      log.debug(s"Got an $initOffsets request")
      val newOffsets = offsetsTracker.initOffsets(initOffsets.partitions)
      context.become(trackOffsets(newOffsets))
    case updateProcessedOffsets: UpdateProcessedOffsets =>
      if (log.isDebugEnabled) {
        log.debug(s"Got an $updateProcessedOffsets request")
      }
      val newOffsets = offsetsTracker.getLatestOffsetsFromMessages(
        currentOffsets, updateProcessedOffsets.readOffsets)
      sender ! OffsetsUpdated
      context.become(trackOffsets(newOffsets))
    case ReportLag =>
      log.debug("Got a ReportLag request")
      reportLag(sender, currentOffsets)

    case AssignedPartitions =>
      log.debug("Got an AssignedPartitions request")
      sender() ! currentOffsets.keySet

    case ReportOffsets =>
      sender() ! PartitionsOffsets(currentOffsets)

    case ReportPartitionsStatus(maxPartitions) =>
      sender() ! PartitionsStatus(
        offsetsTracker.getPartitionsStatus(currentOffsets, maxPartitions))

  }

  private def reportLag(
    sender:         ActorRef,
    currentOffsets: Map[MessageOffset.Partition, MessageOffset.Offset]): Unit = {
    log.debug("Got ReportLag message")
    val lagResult = LagResult(offsetsTracker.getLag(currentOffsets))
    log.debug(s"Responded with $lagResult")
    sender ! lagResult
  }
}

/**
 * Companion object for the OffsetsManager class, containing all its supported Akka message
 *  types.
 */
object OffsetsManager {
  // Requests sent to the OffsetsManager actor
  case class InitOffsets(partitions: Seq[MessageOffset.Partition])
  case class UpdateProcessedOffsets(readOffsets: Seq[MessageOffset])
  case object ReportLag
  case object AssignedPartitions
  case object ReportOffsets
  case class ReportPartitionsStatus(maxPartitions: Int)

  // Responses sent back from the OffsetsManager actor
  case object OffsetsUpdated
  case class LagResult(lag: Long)
  case class PartitionsOffsets(offsets: Map[MessageOffset.Partition, MessageOffset.Offset])
  case class PartitionsStatus(partitions: Seq[PartitionStatus])

  val REPORT_OFFSETS_TIMEOUT: FiniteDuration = 1 seconds
}
