package com.zooz.kafkas3connector.flowmanager

import javax.inject.Inject
import play.api.Configuration
import akka.actor.ActorRef
import javax.inject.Named
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.Terminated
import akka.actor.ActorSystem
import akka.actor.CoordinatedShutdown
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Responsible for cleanly shutting down the system in case one of the main actors fail
 */
class DeathWatcher @Inject() (
  conf:                                         Configuration,
  @Named("consumer") consumerActor:             ActorRef,
  @Named("buffers-manager") bufferManagerActor: ActorRef,
  @Named("health-checker") healthCheckerActor:  ActorRef,
  @Named("flow-manager") flowManagerActor:      ActorRef,
  actorSystem:                                  ActorSystem)
  extends Actor with ActorLogging {

  log.info("Starting up DeathWatcher")

  // Registering actor watch on all main actors
  context.watch(consumerActor)
  context.watch(bufferManagerActor)
  context.watch(healthCheckerActor)
  context.watch(flowManagerActor)

  override def receive: Receive = {
    case Terminated(actor) =>
      log.error(s"Actor ${actor} terminated due to exception. Aborting service")
      flowManagerActor ! FlowManager.ShutdownAbort
      abortSystem
  }

  /**
   * Aborting the service in case of an error
   */
  private def abortSystem: Unit = {
    val shouldShutdown = conf.getOptional[Boolean]("app.shutdown_on_error").getOrElse(true)
    val abortTimeoutSecs = conf.get[Int]("app.shutdown_abort_timeout_seconds")
    if (shouldShutdown) {
      log.error("Aborting service")

      log.info("Shutting down Akka system")
      val shutdownTask = 
        CoordinatedShutdown.get(actorSystem).run(CoordinatedShutdown.UnknownReason)
      Await.ready(shutdownTask, abortTimeoutSecs seconds)
    } else {
      val message =
        "Was asked to abort server, but 'shutdown_on_error' is set to 'false'. Throwing an exception instead" // scalastyle:off
      log.info(message)
      throw new RuntimeException(message)
    }
  }
}
