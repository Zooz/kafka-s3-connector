package com.zooz.kafkas3connector.kafka.mocks

import akka.actor.Actor
import akka.actor.Props
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.{ Consumer => RealConsumer }
import akka.actor.ActorLogging

class Consumer(
  returnedMessages: Seq[KafkaMessage] = Seq[KafkaMessage](),
  lag:              Long              = 0L,
  waitMs:           Long              = 0L)
  extends Actor with ActorLogging {

  def receive: Actor.Receive = {
    case RealConsumer.PollMessages =>
      log.debug("Got PollMessages message") 
      Thread.sleep(waitMs)
      val returnedResult = RealConsumer.PollResult(returnedMessages)
      log.debug(s"Sending backup $returnedResult to ${sender}")
      sender ! returnedResult
    case RealConsumer.Commit => 
      sender ! RealConsumer.CommitCompleted
  }
}
