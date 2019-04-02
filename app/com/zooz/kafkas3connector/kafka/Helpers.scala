package com.zooz.kafkas3connector.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import java.nio.ByteBuffer
import play.api.Configuration
import play.api.Logger

/**
 * Helper methods common to different classes in this package
 */
object Helpers {
  val BYTEBUFFER_DESERIALIZER: String =
    "org.apache.kafka.common.serialization.ByteBufferDeserializer"
  
  val logger = Logger(this.getClass)
  
  /**
   * Connects to the kafka cluster (without subscribing to any topic)
   *  @return a KafkaConsumer instance
   */
  def createKafkaConsumer(conf: Configuration): KafkaConsumer[ByteBuffer, ByteBuffer] = {
    val kafkaBootstrap: String = conf.get[String]("kafka.host")
    
    try {
      val properties = new Properties()
      properties.put("bootstrap.servers", kafkaBootstrap)
      properties.put("group.id", conf.get[String]("app.id"))
      properties.put("enable.auto.commit", "false")
      properties.put("key.deserializer", BYTEBUFFER_DESERIALIZER)
      properties.put("value.deserializer", BYTEBUFFER_DESERIALIZER)
      properties.put("max.poll.records", conf.get[String]("kafka.max_poll_records"))
      properties.put("auto.offset.reset", conf.get[String]("kafka.default_startpoint"))
      
      new KafkaConsumer[ByteBuffer, ByteBuffer](properties)
    } catch {
      case e: Exception =>
        logger.error("Failed instantiating a new KafkaConsumer\n{}", e)
        throw e
    }
  }
}
