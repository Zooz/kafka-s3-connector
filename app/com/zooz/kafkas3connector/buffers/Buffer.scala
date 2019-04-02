package com.zooz.kafkas3connector.buffers

import java.io._

import better.files.File
import better.files.File.root
import play.api.{ Configuration, Logger }
import java.nio.ByteBuffer

/**
 * Parent class for all Buffer classes, used to buffer KafkaMessages before flushing them out.
 *
 *  @constructor Creates a new Buffer
 *  @param conf Handle to Play's configuration
 *  @param filename local file to which buffered data is stored before being copied to target
 *         destination
 */
abstract class Buffer(
  conf:     Configuration,
  fileName: String) extends CopyAble with CompressAble {

  private val bufferSizeBytes = conf.get[Int]("app.buffer_size_bytes")
  private val useCache = conf.get[Boolean]("app.use_cache")
  protected val filePath: String = conf.get[String]("app.buffers_local_dir")
  protected val compressTypeClass: Option[CompressType] = getCompressType
  protected val file: File = createLocalFile(filePath, fileName)
  protected val outputStream: OutputStream = createOutputStream(file, useCache)
  private var wasOutputStreamClosed = false // To avoid multiple .close() calls on flush retries
  protected lazy val logger = Logger(this.getClass())
  protected val NEWLINE: Byte = '\n'.toByte

  /**
   * Prints a message to buffer's output stream followed by a newline.
   *
   */
  def println(byteBuffer: ByteBuffer): Unit = {
    try {
      printlnToStream(byteBuffer)
      logger.debug(s"succeed to insert file = [$fileName] to buffer")
    } catch {
      case e: IOException =>
        logger.error("can't write message to buffer ", e)
        throw e
    }
  }

  /**
   * Creates a local temporary file if not exists
   *  If COMPRESS_TYPE is set to non-NONE value - Extension would be added as a suffix to the file
   *  (For example 'gz' would be added if COMPRESS_TYPE is GZIP)
   *
   *  @param filePath local path in which the file will be created
   *  @param fileName name of created file
   */
  private def createLocalFile(filePath: String, fileName: String): File = {
    val fileNameWithExtension = compressTypeClass match {
      case Some(ct) => s"$fileName.${ct.extension}"
      case _        => fileName
    }
    val file: File = (root / filePath / fileNameWithExtension)
      .createFileIfNotExists(createParents = true)
    logger.info(s"Created new file in path [$filePath] with name [$fileNameWithExtension]")
    file
  }

  /**
   * Creates a DataOutputStream from a given file. If compress type is defined - creates
   *  compressed output stream.
   */
  private def createOutputStream(file: File, useCache: Boolean): OutputStream = {
    val outputStream: OutputStream = {
      if (useCache) {
        new BufferedOutputStream(file.newOutputStream(), bufferSizeBytes)
      } else {
        file.newOutputStream()
      }
    }

    compressTypeClass match {
      case Some(ct) => compressStream(outputStream, ct.compressType)
      case _        => outputStream
    }
  }

  /** Prints a string into the buffer's output stream */
  private def printlnToStream(byteBuffer: ByteBuffer): Unit = {
    outputStream.write(byteBuffer.array())
    outputStream.write(NEWLINE)
  }

  /**
   * Returns the compression type based on the COMPRESS_TYPE environment variable:
   *  None if variable is not defined, a CompressTypeClass for a legal value or throws an
   *  IllegalArgumentException if an illegal value.
   */
  private def getCompressType: Option[CompressType] = {
    val compressTypeEnv = conf.getOptional[String]("app.compress_type")
    compressTypeEnv match {
      case Some(compressTypeAsString) =>
        val compressType: Option[CompressType] =
          CompressType.getCompressType(compressTypeAsString)
        compressType match {
          case Some(ct) => Some(ct)
          case None =>
            logger.error(s"not valid compress type $compressTypeAsString")
            throw new IllegalArgumentException("Can't parse compress type")
        }
      case _ => None
    }
  }

  /** Safely closes the output stream */
  private def closeOutputStream: Unit = {
    if (!wasOutputStreamClosed) {
      outputStream.flush()
      outputStream.close()
      wasOutputStreamClosed = true
    }
  }

  /** Flushes the buffer by copying it to the target destination and deleting the local file */
  final def flush(): Unit = {
    closeOutputStream
    copyBuffer()
    file.delete()
  }
}
