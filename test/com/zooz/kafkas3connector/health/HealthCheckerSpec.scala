package com.zooz.kafkas3connector.health

import org.scalatest.FunSpec
import play.api.libs.json.Json
import play.api.libs.json.JsBoolean
import scala.language.postfixOps
import play.api.libs.json.JsObject
import com.zooz.kafkas3connector.health.healthcheckactors.HealthCheckActor
import org.scalatest.BeforeAndAfterAll
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import HealthChecker._
import akka.actor.Props
import com.typesafe.config.ConfigFactory
import akka.actor.ActorLogging
import scala.concurrent.duration._

class HealthCheckerSpec extends TestKit(ActorSystem("HealthCheckerSpec", ConfigFactory.empty()))
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
  "RunAllChecks message" must {
    "Return successful result if all checks succeed" in {

      val expectedDetails = Json.parse("""{
        "success-actor1": {"is_successful": true},
        "success-actor2": {"is_successful": true},
        "success-actor3": {"is_successful": true}
        }""").as[JsObject]

      val healthChecker = system.actorOf(
        Props(new SuccessfulHealthChecker(system)), "SuccessfulHealthChecker")
      healthChecker ! RunAllChecks

      val result = expectMsgType[HealthResult]
      assert(result.isSuccessful == true)
      assert(result.name == "SuccessfulHealthChecker")
      assert(result.details == expectedDetails)
    }

    "Return a failure if one of the checks fail" in {
      val expectedDetails = Json.parse("""{
        "another-success-actor1": {"is_successful": true},
        "another-success-actor2": {"is_successful": true},
        "failed-actor": {"is_successful": false}
        }""").as[JsObject]

      val healthChecker = system.actorOf(Props(new FailedHealthChecker(system)), "FailedHealthChecker")
      healthChecker ! RunAllChecks

      val result = expectMsgType[HealthResult]
      assert(result.isSuccessful == false)
      assert(result.name == "FailedHealthChecker")
      assert(result.details == expectedDetails)
    }
  }

  "Multiple RunAllChecks messages" must {
    "be responded in order after each one completes" in {
      val healthChecker = system.actorOf(Props(new DelayedHealthChecker(system)), "DelayedHealthChecker")
      
      // Sending multiple RunAllChecks messages, even though each test case takes 2 seconds to
      // complete. Expecting the healthchecker to stash and process all RunAllChecks in queue
      healthChecker ! RunAllChecks
      healthChecker ! RunAllChecks
      healthChecker ! RunAllChecks
      
      val expectedDetails = Json.parse("""{
        "delayed-actor1": {"is_successful": true},
        "delayed-actor2": {"is_successful": true}
        }""").as[JsObject]
      val expectedResult = HealthResult(true, "DelayedHealthChecker", expectedDetails)
      
      val results = expectMsgAllClassOf(
          5 seconds, classOf[HealthResult], classOf[HealthResult], classOf[HealthResult])
      
      assert (results.length == 3)
      results.foreach(result => assert(result == expectedResult))
    }
  }
}

class SuccessfulCheck extends HealthCheckActor with ActorLogging {
  def runCheck: HealthResult = {
    log.info(s"Returning successful result")
    HealthResult(true, self.path.name,
      JsObject(Seq("is_successful" -> JsBoolean(true))))
  }
}
class FailedCheck extends HealthCheckActor with ActorLogging {
  def runCheck: HealthResult = {
    log.info(s"Returning faileds result!")
    HealthResult(false, self.path.name,
      JsObject(Seq("is_successful" -> JsBoolean(false))))
  }
}
class DelayedCheck extends HealthCheckActor with ActorLogging {
  def runCheck: HealthResult = {
    Thread.sleep(1000)
    log.info(s"Returning successful result after a delay")
    HealthResult(true, self.path.name,
      JsObject(Seq("is_successful" -> JsBoolean(true))))
  }
}

class SuccessfulHealthChecker(system: ActorSystem) extends HealthChecker(system) {
  override def initActors = Set(
    system.actorOf(Props[SuccessfulCheck], "success-actor1"),
    system.actorOf(Props[SuccessfulCheck], "success-actor2"),
    system.actorOf(Props[SuccessfulCheck], "success-actor3"))
  override def name: String = "SuccessfulHealthChecker"
}

class FailedHealthChecker(system: ActorSystem) extends HealthChecker(system) {
  override def initActors = Set(
    system.actorOf(Props[SuccessfulCheck], "another-success-actor1"),
    system.actorOf(Props[SuccessfulCheck], "another-success-actor2"),
    system.actorOf(Props[FailedCheck], "failed-actor"))
  override def name: String = "FailedHealthChecker"
}

class DelayedHealthChecker(system: ActorSystem) extends HealthChecker(system) {
  override def initActors = Set(
    system.actorOf(Props[DelayedCheck], "delayed-actor1"),
    system.actorOf(Props[DelayedCheck], "delayed-actor2"))
  override def name: String = "DelayedHealthChecker"
}
