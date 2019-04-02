package com.zooz.kafkas3connector.kafka

import scala.language.implicitConversions

/** Case class storing the partition and the offset of a message */
case class MessageOffset (
  partition: MessageOffset.Partition,
  offset: MessageOffset.Offset
)
object MessageOffset {
  type Partition = Int
  type Offset = Long
  
  implicit def kafkaMessagesToMessageOffsets(kafkaMessages: Seq[KafkaMessage]): 
    Seq[MessageOffset] = {
    kafkaMessages.map(message => MessageOffset(message.partition, message.offset))
  }
}
