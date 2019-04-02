package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.partitioners

import com.zooz.kafkas3connector.kafka.KafkaMessage
import org.joda.time.DateTime
import play.api.libs.json.Json
import play.api.libs.json.JsObject
import org.joda.time.format.DateTimeFormat
import play.api.Logger
import com.zooz.kafkas3connector.buffers.Partitioner

/** Paths and Buffers in S3 are all based on the message time. This trait is used to extract
 *  the time from KafkaMessage objects
 */
trait TimePartitioner extends Partitioner{
  val DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH")
  protected lazy val partitionerLogger = Logger(this.getClass())
  
  /** Extracts the timestamp (DateTime) from a kafka message.
   *  The timestamp is part of the buffer key and the output path
   */
  def getDateTimeFromMessage(kafkaMessage: KafkaMessage): Option[DateTime]

  
  /** A safe wrapper around the getDateTimeFromMessage method */
  def extractDateAndHourFromMessage(kafkaMessage: KafkaMessage): (String, String) = {
    try {
      val ts = getDateTimeFromMessage(kafkaMessage)
      if (ts.isDefined) {
        extractDateAndHourFromTs(ts.get)
      } else {
        val errMessage =
          s"""Cannot extract timestamp field from Json message: ${kafkaMessage.value}"""
        throw new IllegalArgumentException(errMessage)
      }
    } catch {
      case e: Exception =>
        partitionerLogger.error(s"Failed parsing json for kafka message: $kafkaMessage", e)
        throw e
    }
  }
  
  /** Splits a given timestamp into a tuple of Date and Hour strings. Needed for 
   *  formatting output paths
   */
  private def extractDateAndHourFromTs(ts: DateTime): (String, String) = {
    // Converts the given timestamp into a "yyyy-MM-dd HH" string
    // and returns the date and the hour sub-strings
    val splitTs: Array[String] = DATETIME_FORMAT.print(ts).split(" ")
    (splitTs(0), splitTs(1))
  }
  
  /** Used by the implementing BufferManager class generate a buffer file based on the message's
   *  date, hour and partition
   */
  def getPartitionKey(kafkaMessage: KafkaMessage): String = {
    val dateAndHour = extractDateAndHourFromMessage(kafkaMessage)
    s"${dateAndHour._1}_${dateAndHour._2}_${kafkaMessage.partition.toString}"
  }

  /** Used by the implementing BufferManager class to place the message's date and hour
   *  inside the S3 path
   */
  def extractKeysForPatternPath(kafkaMessage: KafkaMessage): List[String] = {
    val dateAndHour = extractDateAndHourFromMessage(kafkaMessage)
    List(dateAndHour._1, dateAndHour._2)
  }
}
