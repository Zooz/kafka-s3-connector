package com.zooz.kafkas3connector.utils

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.DateTimeFormatter
import scala.util.Try
import play.api.Logger

/** Some helper class used to properly convert and parse DateTime objects */
object DateTimeUtils {
  lazy val dateTimeLogger = Logger("DateTimeUtils")
  val SUPPORTED_TS_FORMATS = List[DateTimeFormatter](
      DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mmZ'"),
      ISODateTimeFormat.dateTime(),
      ISODateTimeFormat.dateTimeNoMillis())
  
  /** Parses a given timestamp-string based on one of the supported formats */
  def parseTs(tsString: String): Option[DateTime] = {
    val result = SUPPORTED_TS_FORMATS.map(
      format => Try(format.withZoneUTC().parseDateTime(tsString)).toOption).find(_.isDefined)
    if (result.isDefined) {
      result.get
    } else {
      dateTimeLogger.error(s"Illegal timestamp string $tsString")
      None
    }
  }
}
