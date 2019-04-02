package com.zooz.kafkas3connector.kafka.offsets.mocks

import akka.actor.ActorLogging
import akka.actor.Actor
import com.zooz.kafkas3connector.kafka.MessageOffset
import com.zooz.kafkas3connector.kafka.offsets.PartitionStatus

class OffsetsManager extends Actor with ActorLogging {
  import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager._
  override def receive: Receive = {
    case _: InitOffsets => log.info("Got an InitOffsets request")
    case _: UpdateProcessedOffsets =>
      log.debug("Got an UpdateReadOffsets request")
      sender ! OffsetsUpdated
    case ReportLag =>
      log.debug("Got a ReportLag request")
      sender ! LagResult(0)
    case AssignedPartitions =>
      log.debug("Got an AssignedPartitions request")
    case ReportOffsets =>
      log.debug("Got a ReportOffsets request")
      sender ! PartitionsOffsets(Map[MessageOffset.Partition, MessageOffset.Offset](0 -> 0))
    case _: ReportPartitionsStatus =>
      log.debug("Got a ReportPartitionsStatus request")
      sender ! PartitionsStatus(Seq(PartitionStatus(0, 0, 0, 0)))
  }
}