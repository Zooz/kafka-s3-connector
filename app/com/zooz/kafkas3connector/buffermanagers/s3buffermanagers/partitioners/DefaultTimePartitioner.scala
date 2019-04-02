package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.partitioners

import org.joda.time.DateTime
import com.zooz.kafkas3connector.kafka.KafkaMessage

/** Time partitioner used by the DefaultBufferManager class */
trait DefaultTimePartitioner extends TimePartitioner{
  override def getDateTimeFromMessage(kafkaMessage: KafkaMessage): Option[DateTime] = {
    Some(kafkaMessage.creationDateTime)
  }
}
