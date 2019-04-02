package com.zooz.kafkas3connector.health.healthcheckactors

import akka.actor.Actor
import com.zooz.kafkas3connector.health.HealthChecker.HealthResult
import akka.actor.actorRef2Scala
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy

/** A parent class to all actors responsible for running single health checks */
abstract class HealthCheckActor extends Actor {
  import HealthCheckActor._
  
  /** Main actor state: Waiting for a request to run the health check */
  override def receive: Receive = {
    case RunCheck =>
      sender() ! runCheck
  }
  
  def runCheck: HealthResult
  
  /** Setting any unknown exception to escalate to top actor */
  override val supervisorStrategy =
    OneForOneStrategy() {
      case e: Exception => {
        SupervisorStrategy.Escalate
      }
    }
}

/**
 * Companion object to the HealthCheckActor class. Contains all message types supported by this
 * actor
 */
object HealthCheckActor {
  case object RunCheck
}
