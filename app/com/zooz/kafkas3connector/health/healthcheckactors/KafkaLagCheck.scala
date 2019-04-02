package com.zooz.kafkas3connector.health.healthcheckactors

import com.zooz.kafkas3connector.health.HealthChecker.HealthResult
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager.{ ReportLag, LagResult }
import play.api.Configuration
import play.api.libs.json.{ JsObject, JsString, JsNumber }
import org.apache.kafka.common.TopicPartition
import akka.actor.{ ActorRef, Actor, Props }
import akka.util.Timeout
import scala.concurrent.duration._
import scala.util.{ Success, Failure }
import akka.actor.ActorLogging
import akka.actor.Stash
import com.zooz.kafkas3connector.health.healthcheckactors.HealthCheckActor.RunCheck

/** A health check returning the state of the main Kafka Consumer lag.
 *  Note that it does not extend HealthCheckActor like other actors because
 *  of its asynchronous call to the consumer actor, but it responds to the same
 *  Akka message call so it can be used as a HealthChecker child actor.
 *  
 *  @param consumerActor reference to the Kafka Consumer actor (a singleton)
 *  @param conf a handle to Play's configuration
 */
class KafkaLagCheck (
  offsetsManager: ActorRef,
  conf:          Configuration)
  extends Actor with Stash with ActorLogging{
  
  val topic = conf.get[String]("kafka.topic")
  val healthyLag = conf.get[Long]("kafka.healthy_lag")
  val groupId = conf.get[String]("app.id")

  override def receive: Receive = waitForCheckRequest
  
  /** Actor state - waiting for requests to run checks */
  def waitForCheckRequest: Receive = {
    case RunCheck =>  
      offsetsManager ! ReportLag
      context.become(waitForLagResult(sender()))
    case lagResult: LagResult => 
      log.warning(s"Got unexpected LagResult message. Ignoring..") 
  }
  
  /** Actor state - waiting for result from the Kafka consumer to report its lag. any other
   *  request will be stashed until the response is received
   */
  def waitForLagResult(originalSender: ActorRef): Receive = {
    case lagResult: LagResult =>  // Result from the Consumer actor
      val healthResult = HealthResult(
        lagResult.lag <= healthyLag,
        this.getClass.getName,
        JsObject(Seq(
          "description" -> JsString(s"Kafka lag for topic=${topic}, groupId=$groupId"),
          "lag" -> JsNumber(lagResult.lag)))) 
      originalSender ! healthResult
      unstashAll()
      context.become(waitForCheckRequest)
    case RunCheck =>
      // In case we're asked to run the check again while still waiting for a running check's
      // result - delaying the request until done
      stash()
  }
}

/** KafkaLagCheck companion object */
object KafkaLagCheck {
  
  /** Creates a new Props instance used to create an actor reference to this health check */ 
  def apply(consumerActor: ActorRef, conf: Configuration): Props = {
    Props(new KafkaLagCheck(consumerActor, conf))
  }
}
