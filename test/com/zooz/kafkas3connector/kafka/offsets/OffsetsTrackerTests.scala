
package com.zooz.kafkas3connector.kafka.offsets

import org.scalatest.FunSpec
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.{Consumer => ConsumerInterface}
import com.zooz.kafkas3connector.kafka.MessageOffset.{ Partition, Offset }
import java.nio.ByteBuffer
import com.zooz.kafkas3connector.kafka.Consumer
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.OffsetsStartPoint
import org.scalactic.source.Position.apply
import scala.collection.Seq

class OffsetsTrackerTests extends FunSpec {
  import OffsetsTrackerTests._
  import Consumer._
  describe("getAssignedPartitions") {
    it("Should return a sequence of partitions as returned from Kafka") {
      val metadataConsumer = getMockConsumer()
      val offsetsTracker = new OffsetsTracker(
        metadataConsumer, TOPIC, OffsetsStartPoint.earliest)
      val initializedOffsets = offsetsTracker.initOffsets((0 until PARTITIONS))
      val assignedPartitions = initializedOffsets.keys.toList.sorted
      val expecgtedPartitions = (0 until PARTITIONS).toList.toSeq
      assert(assignedPartitions == expecgtedPartitions)
    }
  }
  describe("initOffsets") {
    val metadataConsumer = getMockConsumer()
    describe("For non-committed partitions") {
      it("Should return earliest if set") {
        val offsetsTracker = new OffsetsTracker(
          metadataConsumer, TOPIC, OffsetsStartPoint.earliest)
        val offsets = offsetsTracker.initOffsets(partitionsSeq)
        partitionsSeq.foreach(partition => assert(offsets(partition) == STARTING_OFFSET))
      }
      it("Should return latest if set") {
        val offsetsTracker = new OffsetsTracker(
          metadataConsumer, TOPIC, OffsetsStartPoint.latest)
        val offsets = offsetsTracker.initOffsets(partitionsSeq)
        partitionsSeq.foreach(partition => assert(offsets(partition) == END_OFFSET))
      }
    }
    it("Should return committed offsets") {
      val COMMITTED_OFFSET = 50L
      var committedOffsets = new java.util.HashMap[TopicPartition, OffsetAndMetadata]()
      partitionsSeq.foreach { partition =>
        val topicPartition = new TopicPartition(TOPIC, partition)
        val offsetMetadata = new OffsetAndMetadata(COMMITTED_OFFSET, null)
        committedOffsets.put(topicPartition, offsetMetadata)
      }
      metadataConsumer.commitSync(committedOffsets)
      val offsetsTracker = new OffsetsTracker(
          metadataConsumer, TOPIC, OffsetsStartPoint.latest)
      val offsets = offsetsTracker.initOffsets(partitionsSeq)

      partitionsSeq.foreach(partition => assert(offsets(partition) == COMMITTED_OFFSET))
    }
  }
  describe("getLatestOffsetsFromMessages") {
    val currentOffsets = Map[Partition, Offset](
      1 -> 90L, 2 -> 100L, 3 -> 120L)
    val offsetsTracker = new OffsetsTracker(
        getMockConsumer(), TOPIC, OffsetsStartPoint.latest)
    val messagesRead = Seq[KafkaMessage](
      KafkaMessage(TOPIC, 0, 100L, 0L, "key1".asByteBuffer, "value1".asByteBuffer),
      KafkaMessage(TOPIC, 0, 150L, 0L, "key2".asByteBuffer, "value2".asByteBuffer),
      KafkaMessage(TOPIC, 1, 200L, 0L, "key3".asByteBuffer, "value3".asByteBuffer),
      KafkaMessage(TOPIC, 1, 250L, 0L, "key4".asByteBuffer, "value4".asByteBuffer),
      KafkaMessage(TOPIC, 2, 300L, 0L, "key5".asByteBuffer, "value5".asByteBuffer),
      KafkaMessage(TOPIC, 2, 350L, 0L, "key6".asByteBuffer, "value6".asByteBuffer))
    it("Should pick all offsets based on messages if currentOffsets map is empty") {
      val expectedResult = Map[Partition, Offset](
        0 -> 150L, 1 -> 250L, 2 -> 350L)
      val mergedMap = offsetsTracker.getLatestOffsetsFromMessages(Map[Partition, Offset](), messagesRead)
      assert(expectedResult.toSeq.diff(mergedMap.toSeq).isEmpty)
    }
    it("Should properly merge with non-empty currentOffsets") {
      val expectedResult = Map[Partition, Offset](
        0 -> 150L, 1 -> 250L, 2 -> 350L, 3 -> 120L)
      val mergedMap = offsetsTracker.getLatestOffsetsFromMessages(currentOffsets, messagesRead)
      assert(expectedResult.toSeq.diff(mergedMap.toSeq).isEmpty)
    }
    it("Should return only currentOffsets of newMessages list is empty") {
      val mergedMap = offsetsTracker.getLatestOffsetsFromMessages(currentOffsets, Seq[KafkaMessage]())
      assert(currentOffsets.toSeq.diff(mergedMap.toSeq).isEmpty)
    }
  }
  describe("getLag") {
    it("Should properly calculate lag based on Kafka and the given partitions") {
      val PARTITION_LAG = 5L
      val offsetsTracker = new OffsetsTracker(
        getMockConsumer(), TOPIC, OffsetsStartPoint.latest)
      val currentOffsets: Map[Partition, Offset] = partitionsSeq.map(
        partition => (partition, END_OFFSET - 1 - PARTITION_LAG)).toMap
      val lag = offsetsTracker.getLag(currentOffsets)
      assert(lag == PARTITIONS * PARTITION_LAG)
    }
  }
}
object OffsetsTrackerTests {
  val TOPIC = "test"
  val PARTITIONS = 10
  val partitionsSeq = (0 to PARTITIONS - 1).toList.toSeq
  val STARTING_OFFSET = 10L
  val END_OFFSET = 100L

  def getMockConsumer(
    offsetsStrategy: OffsetResetStrategy = OffsetResetStrategy.EARLIEST): 
    ConsumerInterface[ByteBuffer, ByteBuffer] = {
    val kafkaConsumer = new MockConsumer[ByteBuffer, ByteBuffer](offsetsStrategy)

    var beginningOffsets = new java.util.HashMap[TopicPartition, java.lang.Long]()
    var endOffsets = new java.util.HashMap[TopicPartition, java.lang.Long]()
    var assignedPartitions = new java.util.ArrayList[TopicPartition]()
    for (partition <- 0 to (PARTITIONS - 1)) {
      val topicPartition = new TopicPartition(TOPIC, partition)
      assignedPartitions.add(topicPartition)
      beginningOffsets.put(topicPartition, STARTING_OFFSET)
      endOffsets.put(topicPartition, END_OFFSET)
    }
    kafkaConsumer.assign(assignedPartitions)
    kafkaConsumer.updateBeginningOffsets(beginningOffsets)
    kafkaConsumer.updateEndOffsets(endOffsets)

    kafkaConsumer
  }
}
