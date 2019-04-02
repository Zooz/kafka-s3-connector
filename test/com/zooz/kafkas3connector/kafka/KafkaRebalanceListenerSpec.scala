package com.zooz.kafkas3connector.kafka

import org.scalatest.BeforeAndAfterAll
import com.zooz.kafkas3connector.testutils.kafka.{ KafkaMock, Producer }
import com.zooz.kafkas3connector.flowmanager.FlowManager
import play.api.Configuration
import io.prometheus.client.CollectorRegistry
import play.api.inject.DefaultApplicationLifecycle
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.buffermanagers.BuffersManager
import com.zooz.kafkas3connector.buffers.Buffer
import play.api.Logger
import org.scalatest.WordSpecLike
import akka.testkit.{ TestKit, ImplicitSender }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import scala.concurrent.duration._
import scala.language.postfixOps
import com.zooz.kafkas3connector.kafka.Consumer._
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager
import com.zooz.kafkas3connector.kafka.MessageOffset.{ Partition, Offset }
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import akka.util.Timeout
import scala.concurrent.Await
import org.joda.time.DateTime
import akka.actor.ActorRef
import Utils.TestActorSystem

class KafkaRebalanceListenerSpec extends TestKit(TestActorSystem.actorySystem)
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  val TEST_TOPIC = "rebalance_test_topic"
  val NUM_PARTITIONS = 10
  val NUM_MESSAGES = 10

  val logger = Logger(this.getClass())
  val kafka = new KafkaMock()
  val askTimeoutDuration = 5 seconds
  implicit val askTimeout = Timeout(askTimeoutDuration)
  val buffersManagerMock1 = system.actorOf(
    Props(new com.zooz.kafkas3connector.buffermanagers.mock.BuffersManager), "buffersManagerMock1")
  val buffersManagerMock2 = system.actorOf(
    Props(new com.zooz.kafkas3connector.buffermanagers.mock.BuffersManager), "buffersManagerMock2")
  lazy val kafkaAdmin = new KafkaAdmin(KafkaMock.KAFKA_BOOTSTRAP)
  lazy val producer = new Producer(KafkaMock.KAFKA_BOOTSTRAP)
  val config = Configuration(
    "app.trigger_time_min" -> "60",
    "app.id" -> "unittests",
    "app.number_of_flush_workers" -> 1,
    "app.flush_timeout_seconds" -> 60,
    "kafka.topic" -> TEST_TOPIC,
    "kafka.max_poll_records" -> 1000,
    "kafka.default_startpoint" -> "earliest",
    "app.buffer_size_bytes" -> 10000,
    "app.use_cache" -> true,
    "app.buffers_local_dir" -> "/tmp",
    "kafka.host" -> "localhost:9092",
    "app.microbatch_interval_ms" -> 10,
    "app.process_loop_timeout_seconds" -> 300)

  logger.debug("Initializing SLF4J logger")

  val offsetsManager1 = system.actorOf(Props(new OffsetsManager(config)))
  val consumer1 = system.actorOf(
    Props(new Consumer(config, buffersManagerMock1, offsetsManager1)), "consumer1")
  Thread.sleep(2000)

  "A consumer" must {
    "read all messages from Kafka when run in single mode" in {
      // Generating some messages and then polling them all in a few read attempts
      generateMessages
      Thread.sleep(1000)

      for (_ <- 1 to 10) {
        consumer1 ! PollMessages
        Thread.sleep(1000)
      }
      val allPollResponses = receiveN(10).map(_.asInstanceOf[Consumer.PollResult]).toList
      val allReadMessages = mergePollResponses(allPollResponses)
      assertAllMessagesRead(allReadMessages)
      Thread.sleep(1000)
    }
  }

  "Kafka Rebalance" must {
    "Trigger a flush of all buffers when new consumer joins" in {
      // Getting the last flush time
      val currentFlushTime = getLastFlush(buffersManagerMock1)

      // Polling with both consumers to allow proper revoking and assigning of partitions
      // (which only happen on consumer's poll)
      val offsetsManager2 = system.actorOf(Props(new OffsetsManager(config)))
      val consumer2 = system.actorOf(
        Props(new Consumer(config, buffersManagerMock2, offsetsManager2)), "consumer2")
      Thread.sleep(3000)

      generateMessages
      Thread.sleep(2000)
      for (i <- 1 to 5) {
        consumer2 ! PollMessages
        consumer1 ! PollMessages
        Thread.sleep(1000)
      }
      // We're expecting all the messages to eventually return, but we're not sure
      // at what order
      val allPollResults = receiveN(10).filterNot(_ == CommitCompleted)
        .map(pollResult => pollResult.asInstanceOf[PollResult])
      val allReturnedMessages = mergePollResponses(allPollResults.toList)
      assertAllMessagesRead(allReturnedMessages)

      // We're expecting a rebalance to occur and so a flush to be triggered
      val lastFlushTime = getLastFlush(buffersManagerMock1)
      assert(currentFlushTime.isBefore(lastFlushTime))

      // Making sure the partitions are split equally between the consumers
      offsetsManager1 ! OffsetsManager.AssignedPartitions
      val consumer1AssignedPartitions = expectMsgType[Set[Partition]]

      offsetsManager2 ! OffsetsManager.AssignedPartitions
      val consumer2AssignedPartitions = expectMsgType[Set[Partition]]

      logger.info(s"Consumer1 has the following partitions: $consumer1AssignedPartitions")
      logger.info(s"Consumer2 has the following partitions: $consumer2AssignedPartitions")
      assert(consumer1AssignedPartitions.size == NUM_PARTITIONS / 2)
      assert(consumer2AssignedPartitions.size == NUM_PARTITIONS / 2)
    }
  }

  def mergePollResponses(pollResponses: List[Consumer.PollResult]): List[KafkaMessage] = {
    pollResponses.foldLeft(List[KafkaMessage]()) {
      (finalList, pollResult) =>
        pollResult.messages.toList ::: finalList
    }
  }

  def assertAllMessagesRead(messages: Seq[KafkaMessage]): Unit = {
    assert(messages.length == NUM_MESSAGES)
    messages.sortBy(message => message.value).zip(1 to NUM_MESSAGES).foreach {
      messageAndIndex =>
        assert(messageAndIndex._1.value.asString == generateMessageContent(messageAndIndex._2))
    }
  }

  override def beforeAll() = {
    kafka.start()
    kafkaAdmin.createTopicIfNotExists(TEST_TOPIC, partitions = NUM_PARTITIONS)
    Thread.sleep(5000)
  }

  override def afterAll() = {
    kafka.stop()
    TestKit.shutdownActorSystem(system)
  }

  def generateMessageContent(index: Int): String = {
    s"""{"message-id": "${"%03d".format(index)}"}"""
  }

  def generateMessages = {
    for (i <- 1 to NUM_MESSAGES) {
      producer.produceMessage(TEST_TOPIC, None, generateMessageContent(i))
    }
  }

  def getLastFlush(buffersNamagerMock: ActorRef): DateTime = {
    import com.zooz.kafkas3connector.buffermanagers.mock.BuffersManager.REPORT_LAST_FLUSH_MESSAGE
    Await.result(
      (buffersNamagerMock ? REPORT_LAST_FLUSH_MESSAGE).mapTo[DateTime], askTimeoutDuration)
  }
}
