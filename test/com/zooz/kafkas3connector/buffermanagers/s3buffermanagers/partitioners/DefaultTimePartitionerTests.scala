package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.partitioners

import org.scalatest.FunSpec
import org.joda.time.DateTime
import com.zooz.kafkas3connector.kafka.KafkaMessage
import com.zooz.kafkas3connector.kafka.bytebufferconverters.StringConverters._
import io.prometheus.client.CollectorRegistry
import Utils.BufferTestUtils
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics

import javax.inject.Inject

class DefaultTimePartitionerTests extends FunSpec {
  val timePartitioner = new DefaultTimePartitioner() {}
  
  val today = new DateTime()
  val yesterday = today.plusDays(-1)
  
  describe("Testing getTsStringFromMessage()"){
    it("Message created yesterday should have yesterday's date") {
      testForDate(yesterday)
    }
    it("Message created today should have today's date") {
      testForDate(today)
    }
  }
  
  def testForDate(date: DateTime): Unit = {
    val kafkaMessage = KafkaMessage("topic", 0, 0L, date.getMillis, "key".asByteBuffer, "value".asByteBuffer)
    val ts = timePartitioner.getDateTimeFromMessage(kafkaMessage).get
    assert(ts == date)
  }
}
