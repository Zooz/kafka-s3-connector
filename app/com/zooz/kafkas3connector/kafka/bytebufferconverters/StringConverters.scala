package com.zooz.kafkas3connector.kafka.bytebufferconverters

import scala.language.implicitConversions
import java.nio.ByteBuffer

/**
 * Wrapper class to strings which allows conversion to ByteBuffer
 */
class StringToByteBuffer(str: String) {
  def asByteBuffer: ByteBuffer = {
    ByteBuffer.wrap(str.getBytes("UTF-8"))
  }
}

/**
 * Wrapper class to instances of ByteBuffer which allows conversion to String
 */
class ByteBufferToString(byteBuffer: ByteBuffer) {
  def asString: String = {
    new String(byteBuffer.array(), "UTF-8")
  }
}

/**
 * Extending the String and ByteBuffer classes to allow implicit conversions between the types
 */
object StringConverters {
  implicit def stringToByteBuffer(str: String): StringToByteBuffer = {
    new StringToByteBuffer(str)
  }
  
  implicit def byteBufferToString(byteBuffer: ByteBuffer): ByteBufferToString = {
    new ByteBufferToString(byteBuffer)
  }
}
