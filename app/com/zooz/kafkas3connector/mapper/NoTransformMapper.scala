package com.zooz.kafkas3connector.mapper

import java.nio.ByteBuffer
import com.google.inject.Inject
import play.api.Configuration

/** A basic message mapper that doesn't perform any transformation on its input */
class NoTransformMapper @Inject()(conf: Configuration)
    extends KafkaMessageMapper(conf) {
  override def transformMessage(byteBuffer: ByteBuffer): Seq[ByteBuffer] = {
    Seq(byteBuffer)
  }
}
