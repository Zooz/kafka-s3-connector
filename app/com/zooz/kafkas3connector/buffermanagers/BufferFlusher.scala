package com.zooz.kafkas3connector.buffermanagers

import akka.actor.ActorLogging
import akka.actor.Actor
import com.zooz.kafkas3connector.buffers.Buffer
import akka.actor.Props
import akka.pattern.after
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure
import scala.concurrent.Future
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import akka.actor.ActorRef
import java.util.concurrent.Executors

/**
 * An actor class to flush a buffer.
 *
 *  @constructor Creates a new BufferFlusher actor
 *  @param metrics an instance to the Prometheus MetricCollector to record flush metrics
 */
class BufferFlusher(metrics: BufferManagerMetrics) extends Actor with ActorLogging {
  import BufferFlusher._

  /** Main Akka Receive method */
  def receive: Actor.Receive = {
    case FlushBuffer(buffer, attempt) => {
      try {
        flushBuffer(buffer)
        sender() ! BufferFlushed
      } catch {
        case e: Exception =>
          // Any other flush exception will be sent back to the parent BuffersManager
          // to decide what to do with it
          sender() ! FlushFailed(e, buffer, attempt)
      }
    }
  }

  /**
   * Flushes a buffer (with a possibility of a timeout exception)
   *
   *  @param buffer The buffer to flush
   */
  def flushBuffer(buffer: Buffer): Unit = {
    metrics.singleBufferFlushTimer.time(
      new Runnable {
        override def run(): Unit = buffer.flush()
      })
    metrics.bufferCounter.dec()
  }
}

/** Companion object for the BufferFlusher class */
object BufferFlusher {

  // Requests to the BufferFlusher actor
  case class FlushBuffer(buffer: Buffer, attempt: Int)
  
  // Responses sent from the BufferFlusher actor
  case class FlushFailed(exception: Exception, buffer: Buffer, attempt: Int)
  case object BufferFlushed

  /**
   * Returns a Props instance of a BufferFlusher to be registered as an ActorRef
   *
   *  @param metrics an instance to the Prometheus MetricCollector to record flush metrics
   */
  def apply(metrics: BufferManagerMetrics): Props = {
    Props(new BufferFlusher(metrics))
  }
}
