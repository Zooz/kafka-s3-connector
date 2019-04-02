package com.zooz.kafkas3connector.buffermanagers

import javax.inject.{ Inject, Singleton }
import Utils.{ BufferTestUtils, TestBuffer, UnitSpec }
import better.files.File.root
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import io.prometheus.client.CollectorRegistry
import org.scalatest.mockito.MockitoSugar
import play.api.Configuration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.actor.Props
import BuffersManager._

class BufferManagerMetricsSpec extends TestKit(Utils.TestActorSystem.actorySystem) with ImplicitSender
  with WordSpecLike with BeforeAndAfterAll with MockitoSugar {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "bufferCounter" must {
    "count the number of buffers created" in {
      val newFileName = "key_count"
      val num_of_partitions = 10
      val bufferManagerMetrics = new BufferManagerMetrics(new CollectorRegistry())
      val buffersManager = system.actorOf(
        Props(new BufferManagerMetricsTestClass(
          BufferTestUtils.createConfig(),
          bufferManagerMetrics)), "buffersManager")
      val kafkaMessages = BufferTestUtils.createKafkaMessages(10000, num_of_partitions, newFileName).toSeq
      buffersManager ! InsertMessages(kafkaMessages)
      Thread.sleep(500)
      expectMsg(MessagesInserted)
      assert(bufferManagerMetrics.bufferCounter.get() == 10.0)
      buffersManager ! FlushAllBuffers
      expectMsgType[FlushCompleted]
      assert(bufferManagerMetrics.bufferCounter.get() == 0.0)

      for (j <- 1 to num_of_partitions) {
        val actualFile = root / BuffersManagerSpec.rootFilePath / s"${newFileName}_${j}"
        actualFile.delete()
      }
    }
  }
}
object BufferManagerMetricsSpec {
  val rootFilePath: String = "tmp"
}

class BufferManagerMetricsTestClass(
  configuration: Configuration, metrics: BufferManagerMetrics)
  extends BuffersManager(configuration, metrics) {
  
  override val expectedExceptions: Set[Class[_ <: Exception]] = Set()

  override def getPartitionKey(kafkaMessage: KafkaMessage): String = {
    s"${kafkaMessage.partition}"
  }

  override def createBuffer(kafkaMessage: KafkaMessage): TestBuffer = {
    val partitionKey = getPartitionKey(kafkaMessage)
    new TestBuffer(configuration)(partitionKey, kafkaMessage.key.asString)
  }
  
}
