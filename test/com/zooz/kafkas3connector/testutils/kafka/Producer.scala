package com.zooz.kafkas3connector.testutils.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties
import org.apache.kafka.clients.producer.ProducerRecord
import play.api.Logger

/**
 * Used to produce mock messages to be used in unit tests
 */
class Producer(bootstrap: String) {
  lazy val kafkaProducer = getProducer
  protected val logger = Logger(this.getClass())

  def getProducer: KafkaProducer[String, String] = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", KafkaMock.KAFKA_BOOTSTRAP)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("enable.idempotence", "true")  // To make sure retry until succeeds without dups
    new KafkaProducer(props)
  }

  /**
   * Produces a message to the mock Kafka cluster
   */
  def produceMessage(topic: String, key: Option[String], value: String) = {
    key match {
      case Some(messageKey) => {
        val record = new ProducerRecord[String, String](
          topic, key.get, value)
        kafkaProducer.send(record).get()
      }
      case None => {
        val record = new ProducerRecord[String, String](topic, value)
        kafkaProducer.send(record).get()
      }
    }
    kafkaProducer.flush()
    logger.info(s"Produced message with key=$key, value=$value to topic=$topic")
  }
}