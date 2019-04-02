package com.zooz.kafkas3connector.metrics

import java.io.Writer

/** A helper class used to format Prometheus metrics into a string */
class WriterAdapter(buffer: StringBuilder) extends Writer {

  override def write(charArray: Array[Char], offset: Int, length: Int): Unit = {
    buffer ++= new String(new String(charArray, offset, length).getBytes("UTF-8"), "UTF-8")
  }

  override def flush(): Unit = {}

  override def close(): Unit = {}
}
