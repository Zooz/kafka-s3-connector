package com.zooz.kafkas3connector.flowmanager

import play.api.Configuration
import akka.actor.ActorRef
import akka.testkit.EventFilter
import akka.testkit.TestProbe
import akka.testkit.TestKit
import org.scalatest.WordSpecLike
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import Utils.TestActorSystem
import play.api.inject.DefaultApplicationLifecycle
import io.prometheus.client.CollectorRegistry
import akka.actor.Props

class DeathWatcherSpec extends TestKit(TestActorSystem.actorySystem)
  with ImplicitSender with WordSpecLike with BeforeAndAfterAll {
  val stubActor = system.actorOf(Props[StubActor], "stub-actor")
  val dyingActor = system.actorOf(Props[DyingActor], "dying-actor")

  "Death of watched actors" must {
    "BuffersManager: abort service" in {
      val consumerRef = stubActor
      val buffersManagerRef = dyingActor
      val healthCheckerRef = stubActor
      val flowManagerRef = stubActor
      testWatchedActorDying(consumerRef, buffersManagerRef, healthCheckerRef, flowManagerRef, buffersManagerRef)
    }
    "HealthChecker: abort service" in {
      val consumerRef = stubActor
      val buffersManagerRef = stubActor
      val healthCheckerRef = dyingActor
      val flowManagerRef = stubActor
      testWatchedActorDying(consumerRef, buffersManagerRef, healthCheckerRef, flowManagerRef, healthCheckerRef)
    }
    "Consumer: abort service" in {
      val consumerRef = dyingActor
      val buffersManagerRef = stubActor
      val healthCheckerRef = stubActor
      val flowManagerRef = stubActor
      testWatchedActorDying(consumerRef, buffersManagerRef, healthCheckerRef, flowManagerRef, consumerRef)
    }
    "FlowManager: abort service" in {
      val consumerRef = stubActor
      val buffersManagerRef = stubActor
      val healthCheckerRef = stubActor
      val flowManagerRef = dyingActor
      testWatchedActorDying(consumerRef, buffersManagerRef, healthCheckerRef, flowManagerRef, flowManagerRef)
    }
  }

  /**
   * Asking one of the actors watched by the DeathWatcher to die. Expecting it to shutdown abort
   */
  def testWatchedActorDying(
    consumerRef:       ActorRef,
    buffersManagerRef: ActorRef,
    healthCheckerRef:  ActorRef,
    flowManager:       ActorRef,
    dyingActor:        ActorRef): Unit = {
    val conf = Configuration("app.shutdown_on_error" -> false)

    val probe = TestProbe()
    val deathWatcher = probe.childActorOf(
      Props(new DeathWatcher(conf, consumerRef, buffersManagerRef, healthCheckerRef, flowManager, system)))
    probe.watch(deathWatcher)
    if (flowManager != dyingActor) {
      EventFilter.info(message="Was requested to shutdown abort", occurrences=1) intercept {
        dyingActor ! DyingActor.Die
        Thread.sleep(500)
      }
    }
    probe.expectTerminated(deathWatcher)
  }
}
