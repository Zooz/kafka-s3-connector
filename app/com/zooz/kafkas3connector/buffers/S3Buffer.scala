package com.zooz.kafkas3connector.buffers

import com.zooz.kafkas3connector.s3.S3Client
import play.api.Configuration

/** A Buffer class that supports flushing to S3
 * 
 *  @constructor creates a new S3Buffer instance
 *  @param conf a handle to Play's configuration
 *  @param fileName name of local file to which buffered data is written before flush
 *  @param s3FilePath target destination for the buffer file once it is flushed
 */
class S3Buffer (conf:       Configuration,
                fileName:   String,
                s3FilePath: String) extends Buffer(conf, fileName) {

  /** Copies the local file to S3 */
  override def copyBuffer(): Unit = {
    val s3Client = new S3Client(conf)
    
    val s3FilePathWithExtension  = compressTypeClass match {
      case Some(ct) => s"$s3FilePath.${ct.extension}"
      case None => s3FilePath
    }
    try {
      logger.info(s"start to copy buffer [$fileName] to S3")
      s3Client.uploadFileToS3(s3FilePathWithExtension, file)
      logger.info(s"finish to copy buffer [$fileName] to S3")
    } finally {
      s3Client.close()
    }
  }
}
