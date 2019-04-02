package com.zooz.kafkas3connector.buffers

import com.zooz.kafkas3connector.kafka.KafkaMessage

/** Adds an ability to create buffers */ 
trait   BufferAble {
  
  /** Creates a new buffer */
  def createBuffer(kafkaMessage: KafkaMessage) : Buffer
}

