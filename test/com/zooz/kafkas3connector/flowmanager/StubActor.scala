package com.zooz.kafkas3connector.flowmanager

import akka.actor.ActorLogging
import akka.actor.Actor

class StubActor extends Actor with ActorLogging {
  override def receive: Receive = {
    case FlowManager.ShutdownAbort =>
      log.info("Was requested to shutdown abort")
    case message =>
      log.info(s"StubActor ignores message $message")
  }
}
