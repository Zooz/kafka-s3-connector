package com.zooz.kafkas3connector.buffers

import java.io.OutputStream
import org.apache.commons.compress.compressors.{CompressorOutputStream, CompressorStreamFactory}

/** Adds the ability to compress output streams */
trait CompressAble {

  /** Converts a given output stream into a compressed output stream based on the stream type */
  def compressStream(outputStream: OutputStream, compressType: String): CompressorOutputStream = {
    new CompressorStreamFactory()
      .createCompressorOutputStream(compressType, outputStream)
  }
}
