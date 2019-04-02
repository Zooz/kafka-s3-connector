package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.partitioners

import Utils.UnitSpec
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.JsObjectConverters._
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import io.prometheus.client.CollectorRegistry
import org.junit.Assert._
import org.joda.time.DateTime
import com.zooz.kafkas3connector.utils.DateTimeUtils
import Utils.BufferTestUtils
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics

class DummyTimePartitioner()
  extends TimePartitioner {
    override def getDateTimeFromMessage(kafkaMessage: KafkaMessage): Option[DateTime] = {
      val ts = kafkaMessage.value.asJsObject \ "last_update_system_time"
      if (ts.isDefined) {
        DateTimeUtils.parseTs(ts.as[String])
      } else {
        None
      }
    }
  }

class TimePartitionerTests extends UnitSpec {
  val timePartitioner = new DummyTimePartitioner()
  val validKafkaMessage: KafkaMessage =
    KafkaMessage("Topic", 10, 10, new DateTime().getMillis(), "key".asByteBuffer,
      ("{\"last_update_system_time\":" +
        "\"2018-09-14T16:54:29.020Z\",\"event_type\": \"payment.payment.create\"}").asByteBuffer)

  describe("extractDateAndHour") {
    it("should return date and hour from kafka message") {
      val dateAndHour = timePartitioner.extractDateAndHourFromMessage(validKafkaMessage)
      assertEquals(dateAndHour._1, "2018-09-14")
      assertEquals(dateAndHour._2, "16")
    }
    it("should throw IllegalArgumentException when can't parse Json") {
      val kafkaMessage: KafkaMessage =
        KafkaMessage("Topic", 10, 10, new DateTime().getMillis(), "key".asByteBuffer,
          "{\"action\":\"2018-09-14T16:54:29.020Z\"}".asByteBuffer)
      assertThrows[IllegalArgumentException](
        timePartitioner.extractDateAndHourFromMessage(kafkaMessage))
    }
    it("should throw IllegalArgumentException when can't extract date and hour due to bad format") {
      val kafkaMessage: KafkaMessage =
        KafkaMessage("Topic", 10, 10, new DateTime().getMillis(),
          "key".asByteBuffer, ("{\"last_update_system_time\":" +
            "\"2018-09-1416:54:29.020Z\",\"event_type\": \"payment.payment.create\"}").asByteBuffer)

      assertThrows[IllegalArgumentException](
        timePartitioner.extractDateAndHourFromMessage(kafkaMessage))
    }
  }

  describe("extractArgs") {
    it("should extract the correct args from kafka message") {
      val actualS3Path = timePartitioner.extractKeysForPatternPath(validKafkaMessage)
      val expectedS3Path = List("2018-09-14", "16")
      assertEquals(expectedS3Path, actualS3Path)
    }
  }

  describe("getPartitionKey") {
    val actualPartitionKey = timePartitioner.getPartitionKey(validKafkaMessage)
    val expectedPartitionKey = "2018-09-14_16_10"
    assertEquals(expectedPartitionKey, actualPartitionKey)
  }
}
