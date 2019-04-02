package com.zooz.kafkas3connector.kafka.offsets

import org.apache.kafka.clients.consumer.{ Consumer => ConsumerInterface }
import com.zooz.kafkas3connector.kafka.OffsetsStartPoint
import play.api.Logger
import com.zooz.kafkas3connector.kafka.MessageOffset
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import java.nio.ByteBuffer

/**
 * Functions used to track the offset read. Used by the OffsetsManager actor
 * (functions are placed outside of the actor for ease of unit testing)
 *
 * @param metadataConsumer KafkaConsumer instance used to get the latest/earliest topics for
 *        the partition
 * @param topic the topic messages are read from
 * @param kafakStartPoint on initialization - if no committed offsets are found, whether to start
 *        at the earliest of latest offset for the topic
 */
class OffsetsTracker(
  metadataConsumer: ConsumerInterface[ByteBuffer, ByteBuffer],
  topic:            String,
  kafkaStartPoint:  OffsetsStartPoint) {
  val logger = Logger(this.getClass)

  /** Converts a list of Partition indexes (Long) into Kafka's TopicPartition instances */
  private def asTopicPartitions(
    partitions: Seq[MessageOffset.Partition]): java.util.Collection[TopicPartition] = {
    partitions.map(
      partition => new TopicPartition(topic, partition)).asJava
  }

  /** Converts a kafka metadata map, to a scala Map of Partition (Int) and Offset (long) */
  private def partitionsOffsetsMap(
    kafkaMetadata: java.util.Map[TopicPartition, java.lang.Long]): 
    Map[MessageOffset.Partition, MessageOffset.Offset] = {
    kafkaMetadata.asScala.map {
      kv => (kv._1.partition -> Long2long(kv._2))
    }.toMap
  }

  /**
   * Runs a given offset metadata function for the given list of Partitions
   * and return a map of partitions and their offsets as extracted from the result.
   *
   * @param partitions a list of partitions to query their metadata
   * @param a kafka native metadata function (such as "beginningOffset" or "endOffset") to apply
   *        on all partitions
   */
  private def getOffsetsMetadata(
    partitions:   Seq[MessageOffset.Partition],
    metadataFunc: (java.util.Collection[TopicPartition]) => 
                   java.util.Map[TopicPartition, java.lang.Long]): 
    Map[MessageOffset.Partition, MessageOffset.Offset] = {
    val offsetsMetadata = Option(metadataFunc(asTopicPartitions(partitions)))
    if (offsetsMetadata.isEmpty) {
      val msg = s"Can't get beginning offsets metadata for partitions $partitions"
      logger.error(msg)
      throw new IllegalArgumentException(msg)
    } else {
      partitionsOffsetsMap(offsetsMetadata.get)
    }
  }

  /** Returns the committed offset for the given partition */
  private def getCommittedOffset(
    partition: MessageOffset.Partition): Option[MessageOffset.Offset] = {
    val topicPartition = new TopicPartition(topic, partition)
    val committedMetadata = Option(metadataConsumer.committed(topicPartition))
    committedMetadata.map(metadata => Long2long(metadata.offset))
  }

  /**
   * Initializing the latest offsets read for the given partitions list. If no committed
   *  offset is found in Kafka for this consumer group - uses either the latest or earliest
   *  offset for this partition instead.
   */
  def initOffsets(partitions: Seq[MessageOffset.Partition]): 
    Map[MessageOffset.Partition, MessageOffset.Offset] = {
    logger.debug(
      s"Initializing latest committed offsets for partitions (${partitions.mkString(", ")})")

    val metadataOffsets =
      kafkaStartPoint match {
        case OffsetsStartPoint.earliest =>
          getOffsetsMetadata(partitions, metadataConsumer.beginningOffsets)
        case OffsetsStartPoint.latest =>
          getOffsetsMetadata(partitions, metadataConsumer.endOffsets)
      }

    // For each partition - either picks the committed offset or the earliest/latest
    val initializedOffsets =
      partitions.map {
        partition =>
          {
            val offset = getCommittedOffset(partition).getOrElse(metadataOffsets(partition))
            logger.debug(s"Initializing offset=$offset for partition=$partition")
            partition -> offset
          }
      }.toMap
    logger.info(s"New initialized offsets: $initializedOffsets")
    initializedOffsets
  }

  /**
   * Updates the latest offsets read based on a given list of newly-read kafka messages
   *
   *  @param currentOffsets the current map of partitions and their latest read offsets
   *  @param newMessages a sequence of newly read Kafka messages
   *  @return a new map of partitions and their latest read offsets which includes the new
   *          messages
   */
  def getLatestOffsetsFromMessages(
    currentOffsets: Map[MessageOffset.Partition, MessageOffset.Offset],
    newOffsets:     Seq[MessageOffset]): Map[MessageOffset.Partition, MessageOffset.Offset] = {
    // Grouping all messages by their partition, identifies the message with the highest
    // offset and generating a map of partition and its latest offset
    val offsetsByPartition: Map[MessageOffset.Partition, Seq[MessageOffset]] =
      newOffsets.groupBy(messageOffset => messageOffset.partition)

    val maxPartitionOffset: Map[MessageOffset.Partition, MessageOffset] =
      offsetsByPartition.mapValues(messageOffset => messageOffset.maxBy(_.offset))

    val latestOffsetByPartition: Map[MessageOffset.Partition, MessageOffset.Offset] =
      maxPartitionOffset.map(partitionAndLatestOffset =>
        partitionAndLatestOffset._1 -> partitionAndLatestOffset._2.offset)

    // Merging the latest offsets map extracted from the new message with
    // the current offsets of all known partitions
    (currentOffsets.keys ++ latestOffsetByPartition.keys).map {
      partition =>
        (partition -> latestOffsetByPartition.getOrElse(partition, currentOffsets(partition)))
    }.toMap
  }

  /**
   * Calculates the total lag as the difference between the current offsets and the latest
   *  offsets registerd in Kafka
   *
   * @param currentOffsets a map of all partitions and the offset read from them
   */
  def getLag(currentOffsets: Map[MessageOffset.Partition, MessageOffset.Offset]): Long = {
    val partitions = currentOffsets.keys.toList
    val endOffsets = getOffsetsMetadata(partitions, metadataConsumer.endOffsets)
    partitions.map {
      partition =>
        {
          val partitionLag = (endOffsets(partition) - 1) - currentOffsets(partition)
          if (partitionLag > 0) partitionLag else 0
        }
    }.sum
  }

  /**
   * Returns all the partitions managed by this instance together with their read and
   * last offsets
   *
   * @param currentOffsets a map of all partitions and the offset read from them
   * @param maxPartitions maxiumum number of partitions to return
   * @return A sequence of PartitionStatus objects, containing a partition, its read offset
   *         and its last offset registerd in Kafka
   */
  def getPartitionsStatus(
    currentOffsets: Map[MessageOffset.Partition, MessageOffset.Offset],
    maxPartitions:  Int): Seq[PartitionStatus] = {
    val partitions = currentOffsets.keys.toList.sorted
    val endOffsets = getOffsetsMetadata(partitions, metadataConsumer.endOffsets)
    partitions.map {
      partition =>
        {
          val currentOffset: MessageOffset.Offset = currentOffsets(partition)
          val endOffset : MessageOffset.Offset = 
            if ((endOffsets(partition) - 1L) >= currentOffset) { 
              (endOffsets(partition) - 1L) 
            } else {
              endOffsets(partition)    
            }
                    
            PartitionStatus(partition, currentOffset, endOffset, endOffset - currentOffset)
        }
    }.toSeq.take(maxPartitions)
  }
}
