package com.zooz.kafkas3connector.health

import play.api.libs.json.{JsObject, Json}
import com.zooz.kafkas3connector.health.healthcheckactors.HealthCheckActor
import akka.actor.Actor
import akka.actor.ActorLogging
import javax.inject.Inject
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Stash
import akka.actor.Terminated

/** Parent class to all Health Checker actors used to report the system's health
 *  @param system the Actor system used to create the child health actors
 */
abstract class HealthChecker @Inject() (system: ActorSystem) 
extends Actor with ActorLogging with Stash{
  import HealthChecker._
  
  val healthCheckActors: Set[ActorRef] = 
    initActors.map(actor => context.watch(actor))
  
  /** Initializes all the health check actors that are part of this healthchecker.  
   */
  def initActors: Set[ActorRef]
  
  /** The healthchecker name, as should appear in the result
   */
  def name: String = "HealthChecker"

  override def receive: Receive = waitForHealthRequests
  
  /** Actor state waiting for a request to run all the health checks
   */
  def waitForHealthRequests: Receive = {
    // Request to run all checks. This will trigger a request to all health actors
    // to run their checks and switch to an actor state waiting for their results
    case RunAllChecks => {
      val initialResult: HealthResult = HealthResult(true, name, JsObject.empty)
      
      healthCheckActors.foreach(checkActor => checkActor ! HealthCheckActor.RunCheck)
      context.become(waitForCheckResults(sender(), healthCheckActors, initialResult))
    }
    case healthResult: HealthResult =>
      // This is unexpected - prints warning to log
      log.warning(s"Got an unexpected HealthResult while no checks are running: $healthResult")
  }
  
  /** Actor state waiting for check results from all the child HealthCheckActors. It is 
   *  recursively called for every response from one of the health actors.
   *  
   *  @param originalSender reference to the actor the requested the health check
   *  @param pendingResults a set of actors that have not yet reported on their health
   *  @param aggregatedResult the final result to be returned to the requester and to which 
   *         the new health result is to be added.
   */
  def waitForCheckResults(
      originalSender: ActorRef, 
      pendingResults: Set[ActorRef],
      aggregatedResult: HealthResult): Receive = {
    case healthResult: HealthResult =>
      // Adding the result of the newly received health check to the total results
      val newAggregatedResult = HealthResult(
          aggregatedResult.isSuccessful && healthResult.isSuccessful,
          aggregatedResult.name,
          aggregatedResult.details + (healthResult.name -> healthResult.details))
      val newPendingResult = pendingResults - sender()
          
      // If all results were processed - returning the final result to the original
      // sender and switching to a waiting state. Otherwise - removes the response 
      // sender from the list of pending results and keep waiting
      if (newPendingResult.isEmpty) {
        originalSender ! newAggregatedResult
        unstashAll
        context.become(waitForHealthRequests)
      } else { 
        context.become(waitForCheckResults(originalSender, newPendingResult, newAggregatedResult))
      }
      
    case RunAllChecks =>
      // In case we get another request to run all heatlh checks - we stash it
      // until the current checks are finished
      log.info("Got request to run healthchecks while still processing current case. Will be processed when ready")  // scalastyle:ignore 
      stash()
      
    case Terminated(actor) =>
      log.error(s"Health-checker actor $actor died due to exception. Shutting down service")
      context.stop(self)
  }
}

/** Companion object for the HealthChecker class containing the list of supported Akka message
 *  types
 */
object HealthChecker {
  
  // Requests to the HealthChecker actor
  case object RunAllChecks
  
  // Responses sent back from the HealthCehcker actor
  case class HealthResult(isSuccessful: Boolean, name: String, details: JsObject){
    override def toString = 
      s"HealthResult(isSuccessful: $isSuccessful, name: $name, details: $details)"
  }
}
