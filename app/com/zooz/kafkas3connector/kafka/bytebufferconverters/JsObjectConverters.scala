package com.zooz.kafkas3connector.kafka.bytebufferconverters

import play.api.libs.json.JsObject
import java.nio.ByteBuffer
import scala.language.implicitConversions
import play.api.libs.json.Json

/**
 * Wrapper class to instances of JsObject which allows conversion to ByteBuffer 
 */
class JsObjectToByteBuffer(jsObject: JsObject) {
  def asByteBuffer: ByteBuffer = {
    new StringToByteBuffer(jsObject.toString()).asByteBuffer
  }
}

/**
 * Wrapper class to instances of ByteBuffer which allows conversion to JsObject
 */
class ByteBufferToJsObject(byteBuffer: ByteBuffer) {
  def asJsObject: JsObject = {
    Json.parse(new ByteBufferToString(byteBuffer).asString).as[JsObject]
  }
}

/**
 * Extending the JsObject and ByteBuffer classes to allow implicit conversions between the types
 */
object JsObjectConverters {
  implicit def jsObjectToByteBuffer(jsObject: JsObject): JsObjectToByteBuffer = {
    new JsObjectToByteBuffer(jsObject)
  }
  
  implicit def byteBufferToJsObject(byteBuffer: ByteBuffer): ByteBufferToJsObject = {
    new ByteBufferToJsObject(byteBuffer)
  }
}
