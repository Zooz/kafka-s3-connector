package com.zooz.kafkas3connector.flowmanager.mocks

import akka.actor.Actor
import org.joda.time.DateTime
import com.zooz.kafkas3connector.flowmanager.FlowManager.{InitiateFlush, QueryLastFlushTime, LastFlushTime}
import akka.actor.ActorLogging

class FlowManager extends Actor with ActorLogging {
  override def receive: Receive = waitForFlush(DateTime.now())
  
  def waitForFlush(lastFlushTime: DateTime): Receive = {
    case InitiateFlush => 
      log.info("Got an InitiateFlush message")
      Thread.sleep(1000)
      val now = DateTime.now()
      sender ! LastFlushTime(now)
      context.become(waitForFlush(now))
    case QueryLastFlushTime => sender ! LastFlushTime(lastFlushTime)
  }
}
