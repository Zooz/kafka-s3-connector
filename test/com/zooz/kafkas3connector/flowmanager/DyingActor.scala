package com.zooz.kafkas3connector.flowmanager

import akka.actor.ActorLogging
import akka.actor.Actor

class DyingActor extends Actor with ActorLogging{
  override def receive: Receive = {
    case DyingActor.Die =>
      log.info("Was asked to die on a horrible exception")
      context.stop(self)
    case FlowManager.ShutdownAbort =>
      log.info("Was requested to shutdown abort")
  }
}

object DyingActor {
  case object Die
}
