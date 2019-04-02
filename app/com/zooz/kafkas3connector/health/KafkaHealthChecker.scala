package com.zooz.kafkas3connector.health

import com.zooz.kafkas3connector.health.healthcheckactors.KafkaLagCheck
import javax.inject.{ Inject, Named }
import play.api.Configuration
import akka.actor.ActorRef
import akka.actor.ActorSystem

/**
 * A HealthChecker that includes the KafkaLagCheck health-check.
 *  @param consumerActor a reference to Kafka's Consumer actor (a singleton)
 *  @param conf a handle to Play's configuration
 *  @param actorSystem main Actor System used to create the child health actors
 */
class KafkaHealthChecker @Inject() (
  @Named("offsets-manager") offsetsManager: ActorRef,
  conf:                                     Configuration,
  actorSystem:                              ActorSystem)
  extends HealthChecker(actorSystem) {
  override def initActors: Set[ActorRef] = Set(
    actorSystem.actorOf(KafkaLagCheck(offsetsManager, conf), "KafkaLagCheck"))
}
