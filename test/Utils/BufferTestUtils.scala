package Utils

import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import play.api.Configuration

import scala.collection.mutable.ListBuffer
import org.joda.time.DateTime
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import io.prometheus.client.CollectorRegistry

object BufferTestUtils {
  val DEFAULT_MESSAGE_CONTENT =
    "{\"last_update_system_time\":" +
      "\"2018-09-14T16:54:29.020Z\",\"event_type\": \"payment.payment.create\"}"

  def createKafkaMessages(
    numberOfMessages: Int,
    numberOfBuffers:  Int,
    newFileName:      String,
    content:          String = DEFAULT_MESSAGE_CONTENT): ListBuffer[KafkaMessage] = {
    val kafkaMessages: ListBuffer[KafkaMessage] = ListBuffer.empty
    for (j <- 1 to numberOfBuffers) {
      for (i <- 1 to numberOfMessages / numberOfBuffers) {
        kafkaMessages += KafkaMessage(
          "topic",
          j,
          i.toLong,
          new DateTime().getMillis(),
          s"${newFileName}_${j}".asByteBuffer,
          content.asByteBuffer)
      }
    }
    kafkaMessages
  }

  def createConfig(overrideParams: Map[String, Any] = Map[String, Any]()): Configuration = {
    val defaultParams = Map[String, Any](
      "app.id" -> "unit_test",
      "app.buffer_size_bytes" -> 8192,
      "app.max_flush_retries" -> 3,
      "app.flush_timeout_seconds" -> 60,
      "app.buffers_local_dir" -> "tmp",
      "app.number_of_flush_workers" -> 10,
      "app.use_cache" -> "true",
      "app.shutdown_on_error" -> false,
      "s3.pattern_path" -> "",
      "s3.root_path" -> "",
      "s3.extension" -> "")
    val finalParams: List[(String, Any)] =
      (defaultParams.keys ++ overrideParams.keys).map(
        param => (param -> overrideParams.getOrElse(param, defaultParams(param)))).toList
    Configuration(finalParams: _*)
  }

  def createBufferManagerMetrics: BufferManagerMetrics = {
    new BufferManagerMetrics(new CollectorRegistry())
  }

}
