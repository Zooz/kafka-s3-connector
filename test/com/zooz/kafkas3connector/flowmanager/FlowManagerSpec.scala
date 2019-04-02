package com.zooz.kafkas3connector.flowmanager

import com.zooz.kafkas3connector.kafka.{ Consumer, KafkaMessage }
import io.prometheus.client.CollectorRegistry
import play.api.Configuration
import play.api.Logger
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.WordSpecLike
import org.scalatest.BeforeAndAfterAll
import Utils.TestActorSystem
import com.zooz.kafkas3connector.kafka.mocks.{ Consumer => ConsumerMock }
import com.zooz.kafkas3connector.buffermanagers.mock.{ BuffersManager => BuffersManagerMock }
import akka.actor.Props
import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.duration._
import scala.language.postfixOps
import com.zooz.kafkas3connector.mapper.NoTransformMapper
import FlowManager._
import akka.util.Timeout
import scala.concurrent.Await
import org.joda.time.Seconds
import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import akka.testkit.TestProbe
import akka.testkit.EventFilter

class FlowManagerSpec extends TestKit(TestActorSystem.actorySystem)
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  val SHUTDOWN_PENDING_SLEEP = 500L
  val LOOP_TIMEOUT_SECS = 7

  protected val logger = Logger(this.getClass())
  val timeoutDuration = 5 seconds
  implicit val askTimeout = Timeout(timeoutDuration)

  val buffersManagerMock = system.actorOf(Props[BuffersManagerMock], "buffers-manager")
  val offsetsManagerMock = system.actorOf(
      Props[com.zooz.kafkas3connector.kafka.offsets.mocks.OffsetsManager], "offsets-manager")

  logger.debug("Initializing SLF4J logger (do not remove)")

  "Flushes" must {
    val consumerMock = system.actorOf(Props(new ConsumerMock()), "consumer")

    "be initiated on regular intervals" in {
      val flushIntervalMs = 1000L
      val expectedFlushes = 2
      val flowManager = createFlowManagerActor(consumerMock, buffersManagerMock, flushIntervalMs = flushIntervalMs)
      val initialFlush = Await.result((flowManager ? QueryLastFlushTime).mapTo[LastFlushTime], timeoutDuration)
      Thread.sleep(flushIntervalMs * (expectedFlushes + 1))
      val latestFlush = Await.result((flowManager ? QueryLastFlushTime).mapTo[LastFlushTime], timeoutDuration)

      // Expecting at least 3 flushes to occur during the time slept
      val diffSeconds = (Seconds.secondsBetween(initialFlush.flushTime, latestFlush.flushTime)).getSeconds
      val totalFlushes = (diffSeconds * 1000) / flushIntervalMs
      assert(totalFlushes >= expectedFlushes)
      deleteActor(flowManager)
    }
    "be initiated by a shutdown" in {
      val flushIntervalMs = 10000L
      val flowManager = createFlowManagerActor(consumerMock, buffersManagerMock, flushIntervalMs = flushIntervalMs)
      val initialFlush = Await.result((flowManager ? QueryLastFlushTime).mapTo[LastFlushTime], timeoutDuration)
      Thread.sleep(1000L)
      val shutdownFlush = Await.result((flowManager ? Shutdown).mapTo[LastFlushTime], timeoutDuration)
      assert(shutdownFlush.flushTime.isAfter(initialFlush.flushTime))
      val secondsDiff = (Seconds.secondsBetween(initialFlush.flushTime, shutdownFlush.flushTime)).getSeconds
      assert(secondsDiff < flushIntervalMs / 1000) // Flush was not due to flush interval
      deleteActor(flowManager)
    }
    "wait until current poll (micro-batch) is finished" in {
      val flushIntervalMs = 10000L
      val microbatchIntervalMs = 500L
      val pollSeconds = 3
      val consumerMock = system.actorOf(Props(new ConsumerMock(waitMs = pollSeconds * 1000L)), "delayed-consumer")
      val flowManager = createFlowManagerActor(consumerMock, buffersManagerMock,
        microbatchIntervalMs = microbatchIntervalMs, flushIntervalMs = flushIntervalMs)
      val initialFlush = Await.result((flowManager ? QueryLastFlushTime).mapTo[LastFlushTime], timeoutDuration)
      Thread.sleep(microbatchIntervalMs + 100)
      val shutdownFlush = Await.result((flowManager ? Shutdown).mapTo[LastFlushTime], timeoutDuration)

      // Despite the flush, triggered by the shutdown, was sent almost immediately after the
      // microbatch has started - it will not happen until all results are back from the consumer
      // (3 seconds)
      val secondsDiff = (Seconds.secondsBetween(initialFlush.flushTime, shutdownFlush.flushTime)).getSeconds
      assert(secondsDiff >= pollSeconds)

      deleteActor(flowManager)
    }
  }
  "Hung poll process" must {
    "result in a timeout" in {
      val flushIntervalMs = 10000L
      val microbatchIntervalMs = 500L
      val loopTimeoutSecs = 2

      val consumerMock = system.actorOf(
        Props(new ConsumerMock(waitMs = (loopTimeoutSecs + 1) * 1000L)), "hung-consumer")
      val flowManager = createFlowManagerActor(consumerMock, buffersManagerMock,
        microbatchIntervalMs = microbatchIntervalMs, flushIntervalMs = flushIntervalMs,
        loopTimeoutSecs = loopTimeoutSecs)
      val probe = TestProbe()
      probe.watch(flowManager)
      probe.expectTerminated(flowManager, (loopTimeoutSecs+1) seconds)
    }
  }
  
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def createFlowManagerInstance(
    consumer: ActorRef, buffersManager: ActorRef,
    microbatchIntervalMs: Long,
    flushIntervalMs:      Long,
    loopTimeoutSecs:      Int): FlowManager = {
    val conf = Configuration(
      "app.microbatch_interval_ms" -> microbatchIntervalMs,
      "app.flush_interval_ms" -> flushIntervalMs,
      "app.process_loop_timeout_seconds" -> loopTimeoutSecs)
    val messageMapper = new NoTransformMapper()
    new FlowManager(conf, consumer, buffersManager, offsetsManagerMock, messageMapper,
      CollectorRegistry.defaultRegistry, system)
  }

  /**
   * Asking one of the actors watched by the FlowManager to die. Expecting it to shutdown abort
   */
  def testWatchedActorDying(
    consumerRef:       ActorRef,
    buffersManagerRef: ActorRef,
    dyingActor:        ActorRef): Unit = {
    val conf = Configuration(
      "app.microbatch_interval_ms" -> 10000,
      "app.flush_interval_ms" -> 10000,
      "app.process_loop_timeout_seconds" -> 10000,
      "app.shutdown_on_error" -> false)
    val messageMapper = new NoTransformMapper()

    val probe = TestProbe()
    val flowManager = probe.childActorOf(
      Props(new FlowManager(conf, consumerRef, buffersManagerRef, 
          offsetsManagerMock, messageMapper,
        CollectorRegistry.defaultRegistry, system)))
    probe.watch(flowManager)
    EventFilter[RuntimeException](occurrences = 1) intercept { 
        dyingActor ! DyingActor.Die
      }
  }

  def createFlowManagerActor(
    consumer: ActorRef, buffersManager: ActorRef,
    microbatchIntervalMs: Long = 50L,
    flushIntervalMs:      Long = 5000L,
    loopTimeoutSecs:      Int  = 5): ActorRef =
    {
      system.actorOf(Props(
        createFlowManagerInstance(consumer, buffersManager, microbatchIntervalMs, flushIntervalMs, loopTimeoutSecs)), "flow-manager")
    }

  def deleteActor(actorRef: ActorRef): Unit = {
    watch(actorRef)
    actorRef ! PoisonPill
    expectTerminated(actorRef)
  }
}
