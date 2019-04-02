package com.zooz.kafkas3connector.buffers

/** Adds an ability to copy a buffer */
trait CopyAble {
  def copyBuffer() : Unit
}
