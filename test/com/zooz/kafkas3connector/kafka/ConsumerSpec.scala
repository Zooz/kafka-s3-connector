package com.zooz.kafkas3connector.kafka

import akka.testkit.{ ImplicitSender, TestActors, TestKit }
import org.scalatest.{ BeforeAndAfterAll, WordSpecLike }
import akka.actor.ActorSystem
import com.zooz.kafkas3connector.testutils.kafka.KafkaMock
import com.zooz.kafkas3connector.testutils.kafka.Producer
import akka.actor.ActorRef
import play.api.Configuration
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import Consumer._
import offsets.OffsetsManager
import Utils.TestActorSystem
import play.api.Logger

class ConsumerSpec
  extends TestKit(TestActorSystem.actorySystem)
  with ImplicitSender
  with WordSpecLike with BeforeAndAfterAll {

  val TOPIC = "consumer_test_topic"
  val PARTITIONS = 10
  
  val logger = Logger(this.getClass)
  
  val kafka = new KafkaMock()
  val conf: Configuration = getConf
  val offsetsManager: ActorRef = getOffsetsManager(conf)
  lazy val consumer: ActorRef = startConsumerActor(conf, offsetsManager)
  lazy val kafkaAdmin = new KafkaAdmin(KafkaMock.KAFKA_BOOTSTRAP)
  lazy val producer = new Producer(KafkaMock.KAFKA_BOOTSTRAP)

  "PollMessages" must {
    "return all pending messages from kafka" in {
      val ATTEMPTS = 10
      val MESSAGES = 10
      generateMessages(MESSAGES)
      Thread.sleep(1000)

      var messagesRead = 0
      for (_ <- 0 to ATTEMPTS) {
        if (messagesRead < MESSAGES) {
          consumer ! PollMessages
          val result = expectMsgType[PollResult]
          messagesRead += result.messages.length
          Thread.sleep(1000)
        }
      }
      
      assert(messagesRead == MESSAGES)      
    }
    "return an empty response if no new messages exist" in {
      consumer ! PollMessages
      val result = expectMsgType[PollResult]
      assert(result.messages.length == 0)
    }
  }
  "ReportLag" must {
    "report no lag if there are no new messages" in {
      offsetsManager ! OffsetsManager.ReportLag
      val result = expectMsgType[OffsetsManager.LagResult]
      assert(result.lag == 0)
    }
    "report total lag if not all messages were read" in {
      val MESSAGES = 10
      generateMessages(MESSAGES)
      Thread.sleep(500)
      offsetsManager ! OffsetsManager.ReportLag
      val result = expectMsgType[OffsetsManager.LagResult]
      assert(result.lag == MESSAGES)
    }
  }

  override def afterAll: Unit = {
      TestKit.shutdownActorSystem(system)
    try {
      kafka.stop()
    } catch {
      case e: Exception =>   // Ignoring error on stopping Kafka mock
    }
  }

  override def beforeAll: Unit = {
    kafka.start()
    kafkaAdmin.createTopicIfNotExists(TOPIC, partitions = PARTITIONS)
  }

  def generateMessages(messages: Int) = {
    for (i <- 1 to messages) {
      producer.produceMessage(TOPIC, None, s"""{"message-id": "$i"}""")
    }
  }
  
  def getConf: Configuration = {
    Configuration(
      "kafka.topic" -> TOPIC,
      "kafka.host" -> KafkaMock.KAFKA_BOOTSTRAP,
      "kafka.default_startpoint" -> "earliest",
      "app.id" -> "UnitTests",
      "kafka.max_poll_records" -> 1000,
      "app.flush_timeout_seconds" -> 5)
  }
  
  def getOffsetsManager(conf: Configuration): ActorRef = {
    system.actorOf(Props(new OffsetsManager(conf)), "offsets-manager")
  }

  def startConsumerActor(conf: Configuration, offsetsManagerRef: ActorRef): ActorRef = {
    logger.info("Creating new Consumer actor")
    val flowManagerRef = system.actorOf(
        Props[com.zooz.kafkas3connector.flowmanager.mocks.FlowManager], "flow-manager")
    val consumerRef = system.actorOf(
        Props(new Consumer(conf, flowManagerRef, offsetsManagerRef)), "consumer")

    // Registering the consumer group (will not fetch any messages, but instead
    // just assign Kafka partitions to the new consumer)
    consumerRef ! PollMessages
    expectMsgType[PollResult]
    Thread.sleep(1000)
    
    consumerRef
  }
}
