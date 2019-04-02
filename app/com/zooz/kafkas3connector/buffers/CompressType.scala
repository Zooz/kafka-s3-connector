package com.zooz.kafkas3connector.buffers

import org.apache.commons.compress.compressors.CompressorStreamFactory

/** An enum of supported compression types, mapping the compression name to their 
 *  CompressionStreamFactory and extensions
 */
object CompressType {
  private val compressMapping: Map[String, CompressType] = Map(
    "GZIP" -> CompressType(CompressorStreamFactory.GZIP, "gz"),
    "BZIP2" -> CompressType(CompressorStreamFactory.BZIP2, "bz2"),
    "SNAPPY" -> CompressType(CompressorStreamFactory.SNAPPY_FRAMED, "sz"),
    "LZ4" -> CompressType(CompressorStreamFactory.LZ4_FRAMED, "lz4")
  )

  def getCompressType(compressType: String): Option[CompressType] = {
    compressMapping.get(compressType)
  }
}

case class CompressType(compressType: String, extension: String)
