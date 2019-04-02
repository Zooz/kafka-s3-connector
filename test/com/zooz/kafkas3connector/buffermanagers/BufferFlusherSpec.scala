package com.zooz.kafkas3connector.buffermanagers

import akka.actor.ActorSystem
import akka.testkit.{ ImplicitSender, TestKit }
import org.scalatest.{ BeforeAndAfterAll, WordSpecLike }
import com.zooz.kafkas3connector.buffers.Buffer
import scala.concurrent.duration._
import scala.language.postfixOps
import play.api.Configuration
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import io.prometheus.client.CollectorRegistry
import akka.testkit.EventFilter
import java.util.concurrent.TimeoutException
import com.typesafe.config.ConfigFactory
import akka.testkit.TestActorRef
import scala.concurrent.ExecutionContext
import com.zooz.kafkas3connector.buffermanagers.BuffersManager.FlushTimedOut

class BufferFlusherSpec
  extends TestKit(ActorSystem("BufferFlusherSpec", ConfigFactory.empty()))
  with ImplicitSender
  with WordSpecLike with BeforeAndAfterAll {
  import BufferFlusher._

  val tempDir = Files.createTempDirectory("BufferFlusherSpec")

  val conf = Configuration(
    "app.buffer_size_bytes" -> 1024,
    "app.use_cache" -> false,
    "app.buffers_local_dir" -> tempDir.toString())

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    FileUtils.deleteDirectory(tempDir.toFile())
  }

  "FlushBuffer" must {
    val metrics = new BufferManagerMetrics(CollectorRegistry.defaultRegistry)

    "send back BufferFlushed message when flush is finished" in {
      val bufferFlusher = system.actorOf(BufferFlusher(metrics))
      val buffer = new SuccessfulBuffer(conf, "successful_buffer.txt")
      bufferFlusher ! FlushBuffer(buffer, 0)
      expectMsg(BufferFlushed)
    }
    "send back FlushFailed with the exception of a failing flush" in {
      val bufferFlusher = system.actorOf(BufferFlusher(metrics))
      val buffer = new FailingBuffer(conf, "successful_buffer.txt")
      bufferFlusher ! FlushBuffer(buffer, 0)
      val response = expectMsgType[FlushFailed]
      assert(response.exception.getClass == classOf[RuntimeException])
    }
  }
}

class SuccessfulBuffer(conf: Configuration, filename: String) extends Buffer(conf, filename) {
  override def copyBuffer(): Unit = {
    logger.info("Successfully flushed buffer")
  }
}

class FailingBuffer(conf: Configuration, filename: String) extends Buffer(conf, filename) {
  override def copyBuffer(): Unit = {
    throw new RuntimeException("I've failed!!")
  }
}
