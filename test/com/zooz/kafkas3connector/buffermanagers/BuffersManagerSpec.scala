package com.zooz.kafkas3connector.buffermanagers

import java.util.stream.Collectors
import javax.inject.Inject
import Utils.{ BufferTestUtils, TestBuffer, UnitSpec }
import better.files.File.root
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import io.prometheus.client.CollectorRegistry
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.scalatest.mockito.MockitoSugar
import play.api.Configuration
import scala.collection.JavaConverters._
import scala.io.Source
import org.joda.time.DateTime
import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import akka.actor.Props
import BuffersManager._
import scala.concurrent.duration._
import scala.language.postfixOps
import com.typesafe.config.ConfigFactory
import play.api.Logger
import org.scalatest.BeforeAndAfter
import akka.testkit.TestActorRef
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import com.zooz.kafkas3connector.buffers.Buffer
import mockclasses._
import Utils.TestActorSystem
import akka.testkit.EventFilter
import akka.testkit.TestProbe

class BuffersManagerSpec
  extends TestKit(TestActorSystem.actorySystem)
  with ImplicitSender
  with WordSpecLike with BeforeAndAfterAll with BeforeAndAfter {

  protected val logger = Logger(this.getClass())

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  val COMPRESSION_TYPES = Seq(
    ("GZIP", CompressorStreamFactory.GZIP, "gz"),
    ("SNAPPY", CompressorStreamFactory.SNAPPY_FRAMED, "sz"),
    ("BZIP2", CompressorStreamFactory.BZIP2, "bz2"),
    ("LZ4", CompressorStreamFactory.LZ4_FRAMED, "lz4"))

  "InsertMessages" must {
    "create buffer if buffer doesn't exists and insert messages to the buffer and " +
      "increase the test buffer counter" in {
        val newFileName = "key"
        val bufferManagerMetrics = BufferTestUtils.createBufferManagerMetrics
        val buffersManager = system.actorOf(Props(new BufferManagerTestClass(
          BufferTestUtils.createConfig(),
          bufferManagerMetrics)))
        val kafkaMessages = BufferTestUtils.createKafkaMessages(100, 1, newFileName).toSeq
        buffersManager ! InsertMessages(kafkaMessages)
        logger.debug("Allowing some process time for buffer to be created and messages to be read")
        Thread.sleep(500)
        expectMsg(MessagesInserted)
        assert(bufferManagerMetrics.bufferCounter.get() == 1.0)
        buffersManager ! FlushAllBuffers
        expectMsgType[FlushCompleted]
        assert(bufferManagerMetrics.bufferCounter.get() == 0.0)
        val actualFile = root / BuffersManagerSpec.rootFilePath / kafkaMessages.head.key.asString

        val actualOutput: List[String] =
          actualFile
            .newBufferedReader
            .lines()
            .collect(Collectors.toList())
            .asScala.toList

        actualOutput.foreach(line => {
          assert(kafkaMessages.head.value.asString === line)
        })
      }

    "support non-ascii characters (utf)" in {
      val newFileName = "withHebrew.txt"
      val hebrewText = s"טקסט ניסיון בעברית"
      val buffersManager = system.actorOf(Props(new BufferManagerTestClass(
        BufferTestUtils.createConfig(),
        BufferTestUtils.createBufferManagerMetrics)))
      val kafakMessage = KafkaMessage(
        "topic", 0, 0, new DateTime().getMillis(),
        newFileName.asByteBuffer, s"טקסט ניסיון בעברית".asByteBuffer)
      buffersManager ! InsertMessages(Seq(kafakMessage))
      expectMsg(MessagesInserted)
      buffersManager ! FlushAllBuffers
      expectMsgType[FlushCompleted]
      val actualFile = root / BuffersManagerSpec.rootFilePath / newFileName
      val actualOutput: List[String] =
        actualFile
          .newBufferedReader
          .lines()
          .collect(Collectors.toList())
          .asScala.toList
      assert(actualOutput.length === 1, s"Expected one line but got $actualOutput")
      assert(hebrewText === actualOutput(0))
      actualFile.delete()
    }

    "compress multiple messages in English" in {
      testCompression("Text in English")
    }

    "compress multiple messages in Hebrew" in {
      testCompression("טקסט בעברית")
    }

    "throw exception when COMPRESS_TYPE is defined and invalid" in {
      val buffersManager = TestActorRef(new BufferManagerTestClass(
        BufferTestUtils.createConfig(Map("app.compress_type" -> "INVALID")),
        BufferTestUtils.createBufferManagerMetrics))
      val newFileName = s"newFileName"
      val kafkaMessages = BufferTestUtils.createKafkaMessages(100, 1, newFileName).toSeq
      intercept[IllegalArgumentException] {
        buffersManager.receive(InsertMessages(kafkaMessages))
      }
    }
  }

  "Exceptions during flush" must {
    "cause a retry if from known type. If less than max retry it should succeed" in {
      val retryConf =
        BufferTestUtils.createConfig(Map("test.failing-buffer.failures" -> 2))
      val buffersManager = system.actorOf(Props(new PartlyFailingFlushesBufferManager(
        retryConf, BufferTestUtils.createBufferManagerMetrics)))
      val kafkaMessages = BufferTestUtils.createKafkaMessages(100, 1, "dummy-file").toSeq
      buffersManager ! InsertMessages(kafkaMessages)
      expectMsg(MessagesInserted)
      buffersManager ! FlushAllBuffers
      expectMsgType[FlushCompleted]
    }
    "cause a retry if from known types but timeout after max number of retries" in {
      val timeoutSec = 1
      val maxFailures = 3
      val additionalConf: Map[String, Any] = Map(
        "app.max_flush_retries" -> maxFailures,
        "test.failing-buffer.failures" -> (maxFailures * 2).toString,
        "app.flush_timeout_seconds" -> timeoutSec)
      val retryConf =
        BufferTestUtils.createConfig(additionalConf)

      val probe = TestProbe()
      val buffersManager = probe.childActorOf(Props(new PartlyFailingFlushesBufferManager(
        retryConf, BufferTestUtils.createBufferManagerMetrics)))

      val kafkaMessages = BufferTestUtils.createKafkaMessages(100, 1, "dummy-file").toSeq
      buffersManager ! InsertMessages(kafkaMessages)
      expectMsg(MessagesInserted)
      probe.watch(buffersManager)
      EventFilter.error(message=s"Waited more than $timeoutSec seconds for a flush to complete", 
          occurrences=1) intercept { 
        buffersManager ! FlushAllBuffers
        Thread.sleep(timeoutSec * 3 * 1000)
      }
      probe.expectTerminated(buffersManager)
    }

    "escalate to BufferManager class if from an unknown type" in {
      val probe = TestProbe()
      val buffersManager = probe.childActorOf(Props(new AlwaysFailingFlushesBufferManager(
        BufferTestUtils.createConfig(),
        BufferTestUtils.createBufferManagerMetrics)), "buffersManager")
      val kafkaMessages = BufferTestUtils.createKafkaMessages(1, 1, "dummy-file").toSeq
      buffersManager ! InsertMessages(kafkaMessages)
      expectMsgType[FlushError]
      probe.watch(buffersManager)
      buffersManager ! (FlushAllBuffers)
      probe.expectTerminated(buffersManager)
    }
  }

  def testCompression(testedText: String): Unit = {
    COMPRESSION_TYPES.foreach { line =>
      {
        val (compressType, compressFactoryType, suffix) = (line._1, line._2, line._3)
        val fileName = s"newFileName.$suffix"
        val bufferManagerMetrics = new BufferManagerMetrics(new CollectorRegistry())
        val config = BufferTestUtils.createConfig(Map("app.compress_type" -> compressType))
        val buffersManager = system.actorOf(Props(new BufferManagerTestClass(
          config,
          bufferManagerMetrics)))

        val kafkaMessages = BufferTestUtils.createKafkaMessages(100, 1, fileName, testedText).toSeq
        buffersManager ! InsertMessages(kafkaMessages)
        expectMsg(MessagesInserted)

        buffersManager ! FlushAllBuffers
        expectMsgType[FlushCompleted]

        val actualFile = root / BuffersManagerSpec.rootFilePath / kafkaMessages.head.key.asString
        val factory: CompressorStreamFactory = new CompressorStreamFactory()
        val actualOutput =
          Source
            .fromInputStream(factory.createCompressorInputStream(compressFactoryType, actualFile.newInputStream()))
            .getLines()
            .toList

        actualOutput.foreach(line => {
          assert(kafkaMessages.head.value.asString === line)
        })
        actualFile.delete()
      }
    }
  }
  after {
    val testDir = root / BuffersManagerSpec.rootFilePath
    testDir.delete(true)
  }
}

object BuffersManagerSpec {
  val rootFilePath: String = "tmp"
}
