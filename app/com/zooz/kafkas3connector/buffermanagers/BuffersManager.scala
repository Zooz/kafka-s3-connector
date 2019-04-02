package com.zooz.kafkas3connector.buffermanagers

import scala.concurrent.duration._
import scala.language.postfixOps
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.buffers.{ Buffer, BufferAble, Partitioner }
import akka.actor.{ Actor, ActorLogging, Props, Stash }
import akka.routing.{ Router, ActorRefRoutee, SmallestMailboxRoutingLogic }
import play.api.Configuration
import BuffersManager._
import BufferFlusher._
import scala.concurrent.ExecutionContext
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.routing.SmallestMailboxPool
import akka.actor.Cancellable
import org.joda.time.DateTime
import java.nio.ByteBuffer
import akka.actor.Terminated
import java.util.concurrent.TimeoutException
import akka.routing.Broadcast

/**
 * Parent class for all BuffersManager actors, responsible for buffering input KafkaMessages
 *  before flushing them out.
 *
 *  @constructor Creates a new BufferManager instance and its pool of BufferFlusher instances.
 *  @param conf Handle to Play's configuration
 *  @param metrics an instance to the Prometheus MetricCollector to buffers-related metrics
 */
abstract class BuffersManager(
  conf:    Configuration,
  metrics: BufferManagerMetrics)
  extends Actor with ActorLogging with Stash
  with Partitioner with BufferAble {

  //
  // Override this sequence of exception classes with exceptions that should
  // trigger a retry. All other exceptions during flush will cause the service
  // to abort
  //
  protected val expectedExceptions: Set[Class[_ <: Exception]]

  private val NUMBER_OF_WORKERS: Int = conf.get[Int]("app.number_of_flush_workers")
  private val FLUSH_TIMEOUT: Int = conf.get[Int]("app.flush_timeout_seconds")
  private val MAX_FLUSH_RETRIES: Int = conf.get[Int]("app.max_flush_retries")

  /** Placing the actor in a state of waiting for new messages */
  override def receive: Receive = waitForMessages(Map[String, Buffer]())

  implicit val executionContext = context.system.dispatcher

  // A pool of BufferFlushers to handle the flush operations concurrently
  // and a dedicated ExecutionContext to be passed to them
  private val bufferFlushersPool: Router = initFlusherPool(NUMBER_OF_WORKERS)

  /**
   * Actor state waiting for either more input messages or a request to flush
   *
   *  @param mapBuffer the current map of open buffers
   */
  def waitForMessages(mapBuffer: Map[String, Buffer]): Receive = {
    case InsertMessages(messages) => {
      val newMapBuffer = insertMessages(messages, mapBuffer)
      sender ! MessagesInserted
      context.become(waitForMessages(newMapBuffer))
    }
    case FlushAllBuffers => {
      if (!mapBuffer.keys.isEmpty) {
        flushAllBuffers(mapBuffer)
        val timeoutScheduler = context.system.scheduler.scheduleOnce(
          FLUSH_TIMEOUT seconds, self, FlushTimedOut)
        context.become(waitForAllFlushes(sender, mapBuffer.size, timeoutScheduler))
      } else {
        log.info("Flush requested, but nothing to flush")
        sender ! FlushCompleted(DateTime.now())
      }
    }
    case BufferFlushed => {
      val errorMsg = s"Got an unexpected BufferFlushed message while not waiting for flush to complete. This shouldn't happen! Aborting service" // scalastyle:ignore
      log.error(errorMsg)
      context.stop(self)
    }
    case FlushTimedOut => {
      log.error("Got a FlushTimedOut message while not waiting for a flush to complete")
    }
  }

  /**
   * An actor state - waiting for all buffers to finish flushing
   *
   *  @param flusher a reference for the actor requesting the flush
   *  @param pendingFlushes number of buffers that haven't yet been flushed
   *  @param timeoutScheduler a handle to a FlushTimedOut event. To be cancelled once all
   *         buffers are flushed.
   */
  def waitForAllFlushes(
    flusher: ActorRef, pendingFlushes: Int, timeoutScheduler: Cancellable): Receive = {
    case BufferFlushed => { // A buffer was successfully flushed
      val newPendingFlushes = pendingFlushes - 1
      if (newPendingFlushes == 0) {
        log.info(s"finished to flush all buffers")
        unstashAll()
        timeoutScheduler.cancel()
        metrics.msgInMEMCounter.set(0.0)
        flusher ! FlushCompleted(DateTime.now())
        context.become(waitForMessages(Map[String, Buffer]()))
      } else {
        log.debug(s"Another buffer finished flushing. $newPendingFlushes more to go")
        context.become(waitForAllFlushes(flusher, newPendingFlushes, timeoutScheduler))
      }
    }
    case FlushFailed(exception, buffer, attempt) => // A flush attempt has failed on exception
      if (!isKnownException(exception)) {
        log.error(
          s"Encountered an unexpected exception while trying to flush buffer\n{}",
          exception)
        abortFlush(flusher, exception)
      } else if (attempt >= MAX_FLUSH_RETRIES) {
        log.error(
          s"Too many failures attempting to flush a buffer ($attempt >= $MAX_FLUSH_RETRIES)\n{}",
          exception)
        abortFlush(flusher, exception)
      } else {
        log.warning(
          "Caught an exception while trying to flush a buffer. Retrying...\n{}",
          exception)
        bufferFlushersPool.route(FlushBuffer(buffer, attempt + 1), self)
      }
    case FlushTimedOut =>
      val message = s"Waited more than $FLUSH_TIMEOUT seconds for a flush to complete"
      log.error(message)
      abortFlush(flusher, new TimeoutException(message))
    case Terminated =>
      // BufferFlusher thread has died. This should never happen because all flush exceptions
      // are caught and sent back as FlushFailed message
      val message = "An unexpected error caused a buffer flusher to crash. Stopping service"
      log.error(message)
      abortFlush(flusher, new TimeoutException(message))
    case message =>
      // This shouldn't really happen, as FlowManager is not expected to send any
      // messages until it gets a FlushComplete message, but just in case - we're stashing
      // the message and handling it later
      log.warning(s"Got an unexpected message $message while waiting for flush to complete. Will handle later") // scalastyle:ignore
      stash()
  }

  /**
   * Aborts the current actor and notifies the flusher that the flush has failed
   */
  private def abortFlush(flusher: ActorRef, exception: Exception): Unit = {
    flusher ! FlushError(exception)
    context.stop(self)
  }

  /** 
   *  Checks whether the given flush exception is expected. Expected exceptions will be retried
   *  instead of failed immediately
   */
  private def isKnownException(exception: Exception): Boolean = {
    expectedExceptions.contains(exception.getClass)
  }

  /**
   * Inserting a given list of messages into existing buffers. Creating new buffers if needed.
   *
   *  @param messages a sequence of kafka messages to be buffered
   *  @param mapBuffer current map of opened buffers
   *  @return a map of opened buffers, including all new buffers created after processing
   *          the given messages
   */
  private def insertMessages(
    messages: Seq[KafkaMessage], mapBuffer: Map[String, Buffer]): Map[String, Buffer] = {

    // Inserting all messages into buffers. Generating a new map of buffers as a result
    messages.foldLeft(mapBuffer) {
      (currentMapBuffer, message) =>
        insertMessage(message, currentMapBuffer)
    }
  }

  /**
   * Sending all opened buffers to be flushed by the BufferFlusher pool
   *
   *  @param mapBuffer current map of opened buffers
   */
  private def flushAllBuffers(mapBuffer: Map[String, Buffer]): Unit = {
    log.info(s"start to flush all buffers. Number of buffers [${mapBuffer.size}]")
    mapBuffer.foreach { keyAndBuffer: (String, Buffer) =>
      bufferFlushersPool.route(FlushBuffer(keyAndBuffer._2, 0), self)
    }
  }

  /**
   * Initializes the pool of BufferFlushers
   *
   *  @param numWorkers number of workers in pool
   */
  private def initFlusherPool(numWorkers: Int): Router = {
    val bufferFlushers: Vector[ActorRefRoutee] = Vector.fill(numWorkers) {
      val bufferFlusher = context.actorOf(BufferFlusher(metrics))
      context.watch(bufferFlusher)
      ActorRefRoutee(bufferFlusher)
    }
    Router(SmallestMailboxRoutingLogic(), bufferFlushers)
  }

  /**
   * Buffer a given message
   *
   * @param message     - the message to be buffered
   * @param mapBuffers  - The current buffers map. Message will be inserted into the relevant
   *                      buffer. New buffer will be created if needed.
   * @return mapBuffers - The new buffers map, including the new buffer if one was created.
   */
  protected def insertMessage(
    kafkaMessage: KafkaMessage, mapBuffers: Map[String, Buffer]): Map[String, Buffer] = {
    val partitionKey: String = getPartitionKey(kafkaMessage)
    val bufferOption: Option[Buffer] = mapBuffers.get(partitionKey)
    val message: ByteBuffer = kafkaMessage.value

    metrics.msgInMEMCounter.inc()
    bufferOption match {
      case Some(buffer) =>
        buffer.println(message)
        mapBuffers
      case None =>
        val newBuffer: Buffer = createBuffer(kafkaMessage)
        metrics.bufferCounter.inc()
        newBuffer.println(message)
        log.info(s"add new buffer with key [$partitionKey]")
        mapBuffers + (partitionKey -> newBuffer)
    }
  }
}

/**
 * Companion object for the BuffersManager class, containing the Akka message types the actor
 *  supports
 */
object BuffersManager {
  // Requests sent to the BufferManager actor
  case class InsertMessages(messages: Seq[KafkaMessage])
  case object FlushAllBuffers
  
  // Responses sent by the BufferManager actor
  case object MessagesInserted
  case class FlushCompleted(completionDate: DateTime)
  case object FlushTimedOut
  case class FlushError(exception: Exception)
}
