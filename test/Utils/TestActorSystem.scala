package Utils

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

object TestActorSystem {
  def actorySystem: ActorSystem = {
    ActorSystem(
      "KafkaTests",
      ConfigFactory.parseString("""
akka.test.single-expect-default=7s
akka.loggers = ["akka.testkit.TestEventListener"]
"""))
  }
}
