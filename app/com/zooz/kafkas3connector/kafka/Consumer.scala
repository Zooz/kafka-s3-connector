package com.zooz.kafkas3connector.kafka

import java.util.Properties
import javax.inject.Inject
import org.apache.kafka.clients.consumer.{ ConsumerRecord, KafkaConsumer }
import org.apache.kafka.common.{ PartitionInfo, TopicPartition }
import play.api.Configuration
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.language.postfixOps
import javax.inject.Singleton
import akka.actor.{ Actor, ActorLogging }
import akka.event.Logging
import akka.pattern.ask
import akka.actor.ActorRef
import javax.inject.Named
import java.nio.ByteBuffer
import scala.util.Success
import scala.util.Failure
import MessageOffset._
import scala.concurrent.duration._
import offsets.OffsetsManager._
import offsets.OffsetsManager.UpdateProcessedOffsets
import scala.concurrent.Future
import scala.concurrent.Await
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import play.api.Logger

/**
 * Kafka Consumer actor. Used as the single endpoint for all Kafka related operations
 *  @param conf handle to Play's configuration
 *  @param buffersManagerActor reference to the main BuffersManager actor - used to initiate
 *         flushes in case of a Kafka Partition Rebalance event
 */
class Consumer @Inject() (
  conf:                                          Configuration,
  @Named("buffers-manager") buffersManagerActor: ActorRef,
  @Named("offsets-manager") offsetsManager:      ActorRef)
  extends Actor with ActorLogging {
  import Consumer._

  val topic: String = conf.get[String]("kafka.topic")

  private val rebalanceFlushTimeoutSecs = conf.get[Int]("app.flush_timeout_seconds") * 2
  private val kafkaConsumer: KafkaConsumer[ByteBuffer, ByteBuffer] = createConsumerAndSubscribe

  /**
   * Connects to the kafka cluster and subscribes to the topic
   *  @return a KafkaConsumer instance
   */
  private def createConsumerAndSubscribe: KafkaConsumer[ByteBuffer, ByteBuffer] = {
    val kafkaConsumer = Helpers.createKafkaConsumer(conf)

    log.info(s"Kafka Consumer: Subscribing to topic $topic")
    try {
      kafkaConsumer.subscribe(
        List(topic).asJavaCollection,
        new KafkaRebalanceListener(
          topic, kafkaConsumer, self, buffersManagerActor, offsetsManager,
          rebalanceFlushTimeoutSecs))
      kafkaConsumer
    } catch {
      case e: Exception =>
        log.error(s"Failed subscribing to kafka topic $topic \n{}", e)
        throw e
    }
  }

  /**
   * Polling kafka for new kafka-records
   *  @return a sequence of raw ConsumerRecord objects
   */
  private def pollKafka: Iterable[ConsumerRecord[ByteBuffer, ByteBuffer]] = {
    try {
      kafkaConsumer.poll(NEW_MESSAGES_WAIT_TIME).asScala
    } catch {
      case e: Exception =>
        log.error(s"Caught exception while trying to poll messages from kafka\n{}", e)
        context.stop(self)
        Iterable[ConsumerRecord[ByteBuffer, ByteBuffer]]()
    }
  }

  /** Converts a kafka's ConsumerRecord into a KafkaMessage */
  private def extractMessage(record: ConsumerRecord[ByteBuffer, ByteBuffer]): KafkaMessage = {
    KafkaMessage(
      record.topic(), record.partition(), record.offset(),
      record.timestamp(), record.key(), record.value())
  }

  /**
   * Polls Kafka for new messages and transforming them into a sequence of structured KafkaMessage
   *  objects
   */
  private def readMessages(): Seq[KafkaMessage] = {
    val recordsRead: Iterable[ConsumerRecord[ByteBuffer, ByteBuffer]] = pollKafka
    val kafkaMessages: Seq[KafkaMessage] = recordsRead.map({
      kafkaRecord =>
        extractMessage(kafkaRecord)
    }).toSeq

    if (log.isEnabled(Logging.DebugLevel)) {
      log.debug(s"Read ${kafkaMessages.length} from kafka")
    }

    kafkaMessages
  }

  /**
   * Main actor state.
   *  Supported messages:
   *   PollMessages - poll Kafka and return for more messages
   *   AssignPartitions - Update the partitions managed by this consumer instance
   */
  override def receive: Receive = {
    case PollMessages =>
      log.debug("Got a PollMessage request")
      val messages: Seq[KafkaMessage] = readMessages()

      if (log.isDebugEnabled) {
        log.debug(s"Responding with PollResult with ${messages.length} messages")
      }
      sender ! PollResult(messages)

    case assignPartitions: AssignPartitions =>
      // Initializing latest offsets for the newly assigned partitions
      // and wait for more requests based on them
      log.debug(s"Got an $assignPartitions request")
      offsetsManager ! InitOffsets(assignPartitions.partitions)

    case Commit =>
      log.debug("Got a Commit request")
      val commitSucceeded = commitOffsetsToKafka(topic, offsetsManager, kafkaConsumer)
      if (commitSucceeded) {
        sender ! CommitCompleted
      } else {
        log.error("Aborting service")
        context.stop(self)
      }
  }

  /** On actor shutdown - cleanly shutting down the Kafka connection */
  override def postStop: Unit = {
    try {
      kafkaConsumer.close()
    } catch {
      case e: Exception =>
        log.error("Got exception while trying to close the consumer\n{}", e)
    }
  }
}

/**
 * Companion object for the Consumer class, containing all supported Akka message
 *  types, as well as constants used in the class.
 */
object Consumer {
  val NEW_MESSAGES_WAIT_TIME: Long = 0L // Immediately return if no messages are available
  val logger = Logger(this.getClass)

  // Requests sent to the Consumer actor
  case object PollMessages
  case object Commit
  case class AssignPartitions(partitions: Seq[Partition])

  // Responses sent back from the Consumer actor
  case object CommitCompleted
  case class PollResult(messages: Seq[KafkaMessage])

  /**
   * Synchronously commits all read offsets. The method will query the OffsetsManager actor
   * for the latest offsets, convert them to Kafka format (java) and commit them to Kafka
   *
   *  @param topic The kafka topic messages were consumed from
   *  @param offsetsManager A reference to the OffsetsManager actor
   *  @kafkaConsumer A KafkaConsumer object used to perform the commit 
   *  @return whether succeeded or failed to commit
   */
  def commitOffsetsToKafka(
    topic:             String,
    offsetsManager:    ActorRef,
    kafkaConsumer:     KafkaConsumer[ByteBuffer, ByteBuffer]): Boolean = {
    val currentOffsetsFuture: Future[PartitionsOffsets] =
      ask(offsetsManager, ReportOffsets)(REPORT_OFFSETS_TIMEOUT).mapTo[PartitionsOffsets]
    val currentOffsets = Await.result(currentOffsetsFuture, REPORT_OFFSETS_TIMEOUT)

    // Committed offsets should be higher +1 over the latest read offsets
    val offsetsToCommit: Map[MessageOffset.Partition, MessageOffset.Offset] =
      currentOffsets.offsets.map { partitionAndOffset =>
        partitionAndOffset._1 -> (partitionAndOffset._2 + 1L)
      }.toMap

    val partitionsOffsetsKafkaFormat: java.util.Map[TopicPartition, OffsetAndMetadata] =
      new java.util.HashMap[TopicPartition, OffsetAndMetadata]()

    offsetsToCommit.map {
      partitionAndOffset: (Partition, Offset) =>
        val topicPartition = new TopicPartition(topic, partitionAndOffset._1)

        val offsetAndMetadata = new OffsetAndMetadata(partitionAndOffset._2)
        partitionsOffsetsKafkaFormat.put(topicPartition, offsetAndMetadata)
    }

    try {
      kafkaConsumer.commitSync(partitionsOffsetsKafkaFormat)
      logger.info(s"Committed the following offsets to Kafka: ${offsetsToCommit}")
      true
    } catch {
      case e: Exception =>
        logger.error("Got exception while trying to commit to Kafka\n{}", e)
        false
    }
  }
}
