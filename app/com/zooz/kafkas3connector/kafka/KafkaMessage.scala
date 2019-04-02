package com.zooz.kafkas3connector.kafka

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import play.api.libs.json.JsObject
import java.nio.ByteBuffer

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
}
