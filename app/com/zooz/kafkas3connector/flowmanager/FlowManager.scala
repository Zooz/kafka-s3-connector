package com.zooz.kafkas3connector.flowmanager

import akka.actor.{ ActorLogging, Actor, ActorRef, Stash, Cancellable }
import javax.inject.{ Inject, Singleton, Named }
import play.api.Configuration
import com.zooz.kafkas3connector.buffermanagers.BuffersManager
import org.joda.time.DateTime
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext
import akka.util.Timeout
import com.zooz.kafkas3connector.kafka.Consumer
import akka.pattern.{ ask, pipe }
import scala.concurrent.Future
import com.zooz.kafkas3connector.mapper.KafkaMessageMapper
import scala.util.Success
import scala.util.Failure
import io.prometheus.client.CollectorRegistry
import scala.concurrent.Await
import com.zooz.kafkas3connector.kafka.KafkaMessage
import akka.actor.PoisonPill
import akka.actor.CoordinatedShutdown
import akka.actor.ActorSystem
import akka.Done
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager

/**
 * Main actor, coordinating the reading of messages from kafka, their mapping, their
 *  buffering and flushing
 *
 *  @param conf handle to Play's configuration
 *  @param consumerActor a reference for the Kafka's consumer actor
 *  @param bufferManagerActor a reference for the buffer manager actor
 *  @param messageMapper the mapper used to transform the input messages
 *  @param registry a Prometheus collector registry to which all metrics are published
 *  @param actorSystem Play's Akka Actor System. Used to add registered to coordinated shutdown.
 */
@Singleton
class FlowManager @Inject() (
  conf:                                         Configuration,
  @Named("consumer") consumerActor:             ActorRef,
  @Named("buffers-manager") bufferManagerActor: ActorRef,
  @Named("offsets-manager") offsetsManager:     ActorRef,
  messageMapper:                                KafkaMessageMapper,
  registry:                                     CollectorRegistry,
  actorSystem:                                  ActorSystem)
  extends Actor with ActorLogging with Stash {
  import FlowManager._

  val TRANSACTION_TIMEOUT = 10 seconds

  private val microBatchIntervalMs: Int = conf.get[Int]("app.microbatch_interval_ms")
  private val microBatchIntervalDuration: FiniteDuration = microBatchIntervalMs.milliseconds
  private val flushIntervalMs: Long =
    conf.getOptional[Long]("app.flush_interval_ms").getOrElse(
      conf.get[Long]("app.flush_interval_min") * 60L * 1000L)
  private val processLoopTimeoutSec: Int = conf.get[Int]("app.process_loop_timeout_seconds")
  private val processLoopTimeoutDuration: FiniteDuration = processLoopTimeoutSec seconds

  implicit val executionContext: ExecutionContext = context.system.dispatcher

  /**
   * Actor's initial receive method. Simply initializes all variables and then calls the
   *  main receive loop
   */
  override def receive: Receive = {
    scheduleNextBatch // First message to process is to fetch new messages from Kafka

    mainReceive(
      DateTime.now(),
      scheduleNextFlush,
      Cancellable.alreadyCancelled,
      false)
  }

  /**
   * Main Actor state
   * @param lastFlush: Date of the last successful flush
   * @param nextFlushRequest: Handle to the next send-out of the next InitiateFlush message
   * @param pollTimeout: A cancelled (Cancellable.alreadyCancelled) handle to the next Poll
   * @param pendingKafkaResult: Whether the actor is currently waiting for response from the
   *        Kafka Consumer actor. During this time - shutdown requests will be stashed
   *        timeout.
   */
  def mainReceive(
    lastFlush:          DateTime,
    nextFlushRequest:   Cancellable,
    pollTimeout:        Cancellable,
    pendingKafkaResult: Boolean): Receive = {

    case ProcessNewMessages =>
      log.debug(s"Got a ProcessNewMessages call")
      cancelAll(pollTimeout)
      consumerActor ! Consumer.PollMessages
      val nextPollTimeout: Cancellable = schedulePollTimeout
      context.become(
        mainReceive(lastFlush, nextFlushRequest, nextPollTimeout, true))

    case PollTimeout =>
      if (pendingKafkaResult) {
        val message = s"Waited more then $processLoopTimeoutSec seconds for PollResult to return from the Kafka Consumer. Aborting service" // scalastyle:off
        log.error(message)
        abortSelf
      } else { // Shouldn't happen
        log.warning("Got a PollTimeout message while not in Poll state.. Ignoring")
      }

    case Consumer.PollResult(messages) =>
      if (pendingKafkaResult) {
        log.debug("Got a PollResult with new messages from the Consumer actor")
        cancelAll(pollTimeout)

        bufferMessagesAndUpdateOffsets(messages)

        // Scheduling the next polling of Kafka
        scheduleNextBatch

        unstashAll()  // Unstashing any pending Shutdown request, if such exists
        context.become(mainReceive(
          lastFlush, nextFlushRequest, Cancellable.alreadyCancelled, false))

      } else { // Shouldn't happen
        log.error(
          "Got an unexpected poll result from Consumer. Aborting service to avoid committing")
        abortSelf
      }

    case InitiateFlush =>
      log.debug(s"Got an InitiateFlush call from ${sender}")
      cancelAll(nextFlushRequest, pollTimeout)
      bufferManagerActor ! BuffersManager.FlushAllBuffers
      context.become(waitForFlushComplete(sender, pendingKafkaResult))

    case QueryLastFlushTime =>
      log.debug(s"Got a QueryLastFlushTime call from ${sender}")
      sender ! LastFlushTime(lastFlush)

    case Shutdown =>
      if (!pendingKafkaResult) {
        log.debug(s"Got a Shutdown call from ${sender}")
        cancelAll(nextFlushRequest)
        gracefulShutdown(sender)
      } else {
        // Shutdowns must wait until all pending messages from Kafka have been arrived and
        // processed
        log.info("Will initiate shutdown after results from last poll are done processing")
        stash()
      }

    case ShutdownAbort => // Triggered due to service abort
      cancelAll(nextFlushRequest)
      context.become(ignoreAllMessages)
  }

  /**
   * An actor state waiting for a FlushCompleted message from the BuffersManager actor. All other
   * messages are stashed until the flush is reported to be complete
   *
   * @param flushRequester The actor which requested the flush, to be notified on completion
   * @param pendingKafkaResult Once the flush complete - should the main receive state expect
   *        a PollResult answer from the Kafka Consumer actor?
   */
  def waitForFlushComplete(
    flushRequester:     ActorRef,
    pendingKafkaResult: Boolean): Receive = {
    case flushCompleted: BuffersManager.FlushCompleted =>
      log.debug(s"Asking $consumerActor to Commit")
      val commitCompletedFuture = consumerActor.?(Consumer.Commit)(TRANSACTION_TIMEOUT)
      Await.ready(commitCompletedFuture, TRANSACTION_TIMEOUT)

      val response = LastFlushTime(flushCompleted.completionDate)
      log.debug(s"Got FlushCompleted message from ${sender}. Sending $response to $flushRequester") // scalastyle:ignore
      flushRequester ! response

      unstashAll()

      val nextPollTimeout: Cancellable = // Rescheduling timeout if there are pending messages
        if (pendingKafkaResult) schedulePollTimeout else Cancellable.alreadyCancelled
      val nextFlush: Cancellable = scheduleNextFlush
      context.become(
        mainReceive(DateTime.now(), nextFlush, nextPollTimeout, pendingKafkaResult))

    case BuffersManager.FlushError(exception) =>
      log.error("Flush has failed due to exception. Expecting service to abort.\n{}", exception)

    case ShutdownAbort => // Triggered due to service abort
      context.become(ignoreAllMessages)

    case message =>
      log.debug(s"Stashing $message while waiting for flush to complete")
      stash()
  }

  /**
   * An actor state while in service shutdown abort. In this state - Shutdown messages are
   * immediately responded and any other messages are ignored
   */
  def ignoreAllMessages: Receive = {
    case Shutdown =>
      log.debug(s"Ignoring call for a graceful shutdown during service abort")
      sender ! LastFlushTime(DateTime.now())
    case message => log.error(s"Ignoring message $message during service abort")
  }

  /**
   * Graceful shutdown. Asking BuffersManager to flush all its buffers and waiting until
   * it's completed. When done - sending back confirmation to the given requester
   */
  private def gracefulShutdown(requester: ActorRef) = {
    val flushCompletedFuture: Future[BuffersManager.FlushCompleted] =
      bufferManagerActor.?(BuffersManager.FlushAllBuffers)(processLoopTimeoutDuration)
        .mapTo[BuffersManager.FlushCompleted]
    val lastFlushTimeFuture: Future[LastFlushTime] = flushCompletedFuture.map(
      flushCompleted => LastFlushTime(flushCompleted.completionDate))
    val lastFlushTime = Await.result(lastFlushTimeFuture, processLoopTimeoutDuration)

    val commitCompletedFuture = consumerActor.?(Consumer.Commit)(TRANSACTION_TIMEOUT)
    Await.ready(commitCompletedFuture, TRANSACTION_TIMEOUT)

    requester ! lastFlushTime
  }
  
  /**
   * Synchronously inserting the given sequence of messages to the BufferManager and updates
   * the offsets in the offsets manager
   */
  def bufferMessagesAndUpdateOffsets(messages: Seq[KafkaMessage]): Unit = {
    val mappedMessages: Seq[KafkaMessage] = mapMessagesOrKill(messages)
    val insertCompleteFuture =
      bufferManagerActor.?(BuffersManager.InsertMessages(mappedMessages))(TRANSACTION_TIMEOUT)
    Await.ready(insertCompleteFuture, TRANSACTION_TIMEOUT)
    
    val updateOffsetsFuture = 
      offsetsManager.?(OffsetsManager.UpdateProcessedOffsets(messages))(TRANSACTION_TIMEOUT)
    Await.ready(updateOffsetsFuture, TRANSACTION_TIMEOUT)
  }

  /**
   * Calls the cancel() method for all Cancellable items given
   */
  private def cancelAll(cancellables: Cancellable*): Unit = {
    cancellables.foreach(_.cancel())
  }

  private def registerFlushOnShutdown: Unit = {
    log.debug("Registering a flush on shutdown")
    val shutdownHandle = CoordinatedShutdown.get(actorSystem)
    shutdownHandle.addTask(
      CoordinatedShutdown.PhaseServiceStop, "graceful-shutdown") {
        () => (self.?(Shutdown)(processLoopTimeoutDuration)).map(_ => Done)
      }
  }

  // Shutdown of service will first trigger a flush
  registerFlushOnShutdown

  /**
   * Schedules a message to self to timeout on waiting for a poll result from the
   * kafka Consumer
   */
  private def schedulePollTimeout: Cancellable =
    context.system.scheduler.scheduleOnce(
      processLoopTimeoutDuration, self, PollTimeout)

  /**
   * Schedules a message to self to flush the buffers. This method is called after every
   *  successful flush to schedule a new flush
   */
  private def scheduleNextFlush: Cancellable =
    context.system.scheduler.scheduleOnce(flushIntervalMs milliseconds, self, InitiateFlush)

  /**
   * Schedules another batch of message processing
   */
  private def scheduleNextBatch: Cancellable =
    context.system.scheduler.scheduleOnce(microBatchIntervalDuration, self, ProcessNewMessages)

  /** A wrapper around the messageMapper method to properly catch and handle map exceptions */
  private def mapMessagesOrKill(messages: Seq[KafkaMessage]): Seq[KafkaMessage] = {
    try {
      messageMapper.map(messages)
    } catch {
      case exception: Exception =>
        log.error("Caught exception while trying to map messages:\n{}", exception)
        abortSelf
        Seq[KafkaMessage]()
    }
  }

  /** Called in case of error - aborting self */
  private def abortSelf: Unit = {
    self ! ShutdownAbort
    self ! PoisonPill
  }
}

/** Companion object to the FlowManager class containing supported Akka message types */
object FlowManager {
  // Requests sent to the FlowManager actor (either by external actors or by self)
  case object InitiateFlush
  case object ProcessNewMessages
  case object QueryLastFlushTime
  case object Shutdown
  case object PollTimeout
  case object ShutdownAbort

  // Responses sent back from the FlowManager actor
  case class LastFlushTime(flushTime: DateTime)
}
