package com.zooz.kafkas3connector.health.healthcheckactors

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import com.zooz.kafkas3connector.testutils.kafka.KafkaMock
import com.zooz.kafkas3connector.kafka.Consumer
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager
import com.zooz.kafkas3connector.kafka.KafkaAdmin
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import com.zooz.kafkas3connector.testutils.kafka.Producer
import play.api.libs.json.Json
import play.api.libs.json.JsNumber
import play.api.Configuration
import play.api.Logger
import akka.testkit.TestKit
import org.scalatest.WordSpecLike
import akka.testkit.ImplicitSender
import akka.actor.{ ActorSystem, Props }
import Consumer._
import com.zooz.kafkas3connector.health.HealthChecker._
import com.typesafe.config.ConfigFactory
import Utils.TestActorSystem
import akka.testkit.TestProbe
import com.zooz.kafkas3connector.health.healthcheckactors.HealthCheckActor.RunCheck

class KafkaLagCheckSpec extends TestKit(TestActorSystem.actorySystem)
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  val TEST_TOPIC = "test"
  val HEALTHY_LAG = 10L
  val SLEEP_TIME = 100L
  val MAX_SLEEP_TIME = 5000L
  val kafka = new KafkaMock()
  val testProbe = TestProbe("testProbe")
  val flowManagerMock = testProbe.ref
  lazy val kafkaAdmin = new KafkaAdmin(KafkaMock.KAFKA_BOOTSTRAP)
  lazy val producer = new Producer(KafkaMock.KAFKA_BOOTSTRAP)
  val conf = Configuration(
    "app.id" -> "unittests",
    "kafka.healthy_lag" -> HEALTHY_LAG,
    "kafka.topic" -> TEST_TOPIC,
    "kafka.max_poll_records" -> "1000",
    "kafka.host" -> "localhost:9092",
    "kafka.default_startpoint" -> "latest",
    "app.flush_timeout_seconds" -> 10)
  protected val logger = Logger(this.getClass())
  lazy val offsetsManager = system.actorOf(Props(new OffsetsManager(conf)), "offsetsManager")
  lazy val consumer = system.actorOf(
      Props(new Consumer(conf, flowManagerMock, offsetsManager)), "Consumer")
  lazy val healthChecker = system.actorOf(Props(new KafkaLagCheck(offsetsManager, conf)), 
      "KafkaLagCheck")
  
  var messagesSent = 0

  override def beforeAll() = {
    kafka.start()
    kafkaAdmin.createTopicIfNotExists(TEST_TOPIC)

    // Generating a message and consuming it to register the consumer group
    logger.info("Initializing test and polling kafka to register consumer group")
    producer.produceMessage(TEST_TOPIC, None, "Initializing test")
    consumer ! PollMessages
    val result = expectMsgType[PollResult]
    consumer ! Commit
    expectMsg(CommitCompleted)
  }
  
  "Kafka consumer reporting a lag" must {
    "report a small lag as healthy" in {
      for (i <- 1L to HEALTHY_LAG) {
        sendMessage
      }
      Thread.sleep(500)
      healthChecker ! RunCheck
      val result = expectMsgType[HealthResult]
      assert(
        result.isSuccessful == true, result.details)
    }

    "report a big lag as unhealth" in {
      for (i <- 1 to 5) {
        sendMessage
      }
      Thread.sleep(500)
      healthChecker ! RunCheck
      val result = expectMsgType[HealthResult]
      assert(result.isSuccessful == false, result.details)
    }
    "Report as healthy after an unhelath lag was consumed" in {
      getAllMessage
      healthChecker ! RunCheck
      val result = expectMsgType[HealthResult]
      assert(result.isSuccessful == true, result.details)
    }
  }
  private def sendMessage: Unit = {
    producer.produceMessage(TEST_TOPIC, None, "Message " + messagesSent)
    messagesSent += 1
  }

  /**
   * Keeps consuming messages until last produced message was read
   */
  private def getAllMessage: Unit = {
    val getMessagesBody = () => {
      consumer ! PollMessages
      val result = expectMsgType[PollResult]
      
      // Usually - the FlowManager actor will update the OffsetsManager actor about the updated
      // messages, so in this test we need to make sure of this ourselves
      offsetsManager ! OffsetsManager.UpdateProcessedOffsets(result.messages)
      expectMsg(OffsetsManager.OffsetsUpdated)
      
      result.messages.map{
        message =>
          {
            logger.info("Got message: " + message)
            message.value.asString
          }
      }
    }
    val LAST_MESSAGE = s"Message ${messagesSent - 1}"
    var totalSlept = 0L
    var messagesRead = getMessagesBody()
    while (!messagesRead.contains(LAST_MESSAGE)) {
      Thread.sleep(SLEEP_TIME)
      totalSlept += SLEEP_TIME
      if (totalSlept > MAX_SLEEP_TIME) {
        fail("Timed out waiting to consume " + LAST_MESSAGE)
      }
      messagesRead = getMessagesBody()
    }
  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    try {
      kafka.stop()
    } catch {
      case e: Exception => logger.warn("Caught exception while shutting down Kafka", e)
    }
  }
}