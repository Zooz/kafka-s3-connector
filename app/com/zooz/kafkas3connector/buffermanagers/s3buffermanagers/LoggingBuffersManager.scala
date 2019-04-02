// package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.LoggingBuffersManager

package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers

import javax.inject.Inject
import play.api.Configuration
import javax.inject.Singleton
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.kafka.KafkaMessage
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._

@Singleton
class LoggingBuffersManager @Inject()(
    config: Configuration,
    metrics: BufferManagerMetrics
) extends S3BufferManager(config, metrics) {
  
  val dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")

  override def getDateTimeFromMessage(kafkaMessage: KafkaMessage): Option[DateTime] = {
    val messageAsString: String = kafkaMessage.value.asString

    // Cutting the first 23 characters from the message, containing the timestamp
    val tsString: String = messageAsString.substring(0,23)
    try {
      Some(dateFormatter.parseDateTime(tsString))
    } catch {
      case parseError: java.lang.IllegalArgumentException =>
        // Failed to parse the date - returning None
        None
    }
  }
}
