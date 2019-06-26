package com.zooz.kafkas3connector.mapper

import com.zooz.kafkas3connector.kafka.KafkaMessage
import play.api.Logger
import java.nio.ByteBuffer
import play.api.Configuration

/** A parent trait for all Kafka Message Mappers */
abstract class KafkaMessageMapper(conf: Configuration) {
  protected lazy val logger = Logger(this.getClass())
  protected lazy val skipCorruptedMessages: Boolean = 
    conf.get[Boolean]("app.skip_corrupted_messages")
  
  /** Transforms a given kafka message value to zero or more output strings */
  def transformMessage(byteBuffer: ByteBuffer): Seq[ByteBuffer]

  /** Transforms a sequence of input Kafka messages.
   *  
   *  @param messages a sequence of input messages, as read from Kafka
   *  @return a sequence of messages after they have been mapped (transformed)
   */
  def map(messages: Seq[KafkaMessage]): Seq[KafkaMessage] = {
    if (logger.isDebugEnabled) {
      logger.debug(s"Processing ${messages.length} messages")
    }
    messages.flatMap {
      message =>
        try {
          val transformedMessages = transformMessage(message.value)
          if (!transformedMessages.isEmpty) {
            transformedMessages.map {
              transformedMessage => message.copy(value = transformedMessage)
            }
          } else {
            None
          }
        } catch {
          case e: Exception =>
            val errMsg = s"Caught exception while trying to map message:\n$message"
            logger.error(errMsg, e)
            if (skipCorruptedMessages) {
              logger.info("Skipping corrupted message...")
              None
            } else {
              throw new IllegalArgumentException(errMsg)
            }
        }
    }
  }
}
