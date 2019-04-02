package com.zooz.kafkas3connector.kafka

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.JsObject
import java.nio.ByteBuffer
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._

/** Case class representing a Kafka message */
case class KafkaMessage(
  topic:         String,
  partition:     Int,
  offset:        Long,
  creationEpoch: Long,
  key:           ByteBuffer,
  value:         ByteBuffer) {

  /** Helper method to convert the message creation date (in seconds since epoch) to a
   *  DateTime instance
   */
  def creationDateTime: DateTime = {
    new DateTime(creationEpoch)
  }

  override def toString: String = {
    val keyAsString: String = Option(key).map(_.asString).getOrElse("")
    val valueAsString: String = Option(value).map(_.asString).getOrElse("")
    
    s"KafkaMessage($topic, $partition, $offset, $creationEpoch, ByteBuffer(${keyAsString}), ByteBuffer(${valueAsString}))"  // scalastyle:off
  }
}
