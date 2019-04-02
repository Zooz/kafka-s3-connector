package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers

import com.zooz.kafkas3connector.kafka.KafkaMessage
import play.api.Configuration
import com.zooz.kafkas3connector.buffers.S3Buffer
import com.amazonaws.{ AmazonClientException, AmazonServiceException }
import com.amazonaws.services.s3.model.AmazonS3Exception
import akka.actor.OneForOneStrategy
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.SupervisorStrategy
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.buffermanagers.BuffersManager
import com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.partitioners.TimePartitioner
import javax.inject.Inject

/**
 * Parent class for all BuffersManager actors that flush their buffers to S3
 *
 *  @constructor Creates a new S3BufferManager instance
 *  @param config Handle to Play's configuration
 *  @param metrics an instance to the Prometheus MetricCollector to buffers-related metrics
 */
abstract class S3BufferManager @Inject() (config: Configuration, metrics: BufferManagerMetrics)
  extends BuffersManager(config, metrics) with TimePartitioner {
  private val s3PatternPath = config.get[String]("s3.pattern_path")
  private val s3FileFormat = config.get[String]("s3.extension")

  /** Allowing restarts of the BufferFlushers for known AWS exceptions */
  override val expectedExceptions: Set[Class[_ <: Exception]] = Set(
    classOf[AmazonClientException], classOf[AmazonS3Exception], classOf[AmazonServiceException])

  /**
   * Creating a new S3 buffer based on the root path and the message's partition, offset
   *  and timestamp
   *
   *  @param kafkaMessage input message for which the buffers is to be created
   */
  override final def createBuffer(kafKaMessage: KafkaMessage): S3Buffer = {
    val fileName = getPartitionKey(kafKaMessage)
    val s3PrefixPath = bindArgsToPath(s3PatternPath, extractKeysForPatternPath(kafKaMessage))
    val s3FullFilePath =
      s"$s3PrefixPath/${kafKaMessage.partition}_${kafKaMessage.offset}.$s3FileFormat"
    new S3Buffer(config, fileName, s3FullFilePath)
  }

  /**
   * Formats an S3 path based on a pattern and a list of variables
   *
   *  @param pattern A pattern for an S3 path, with '%s' place holders
   *  @param args list of strings to be placed instead of place holders
   */
  def bindArgsToPath(pattern: String, args: List[String]): String = {
    String.format(pattern, args: _*)
  }
}
