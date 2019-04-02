package com.zooz.kafkas3connector.mapper

import java.nio.ByteBuffer

/** A basic message mapper that doesn't perform any transformation on its input */
class NoTransformMapper extends KafkaMessageMapper{
  override def transformMessage(byteBuffer: ByteBuffer): Seq[ByteBuffer] = {
    Seq(byteBuffer)
  }
}
