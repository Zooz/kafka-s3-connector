package com.zooz.kafkas3connector.buffermanagers.mock

import akka.actor.Actor
import org.joda.time.DateTime
import akka.actor.ActorLogging

class BuffersManager extends Actor with ActorLogging {
  import com.zooz.kafkas3connector.buffermanagers.BuffersManager._
  
     
  
  var lastFlushTime: DateTime = DateTime.now()
  
  def receive: Actor.Receive = {
    case insertMessages: InsertMessages => 
      log.debug(s"Got $insertMessages message")
      sender ! MessagesInserted
    case FlushAllBuffers =>
      log.debug("Got FlushAllBuffers message")
      Thread.sleep(1000)
      lastFlushTime = DateTime.now()
      sender ! FlushCompleted(lastFlushTime)
    case BuffersManager.REPORT_LAST_FLUSH_MESSAGE => sender ! lastFlushTime
  }
}

object BuffersManager {
  // Used to report the latest flush this mock has performed
  val REPORT_LAST_FLUSH_MESSAGE = "reportLastFlush"
}
