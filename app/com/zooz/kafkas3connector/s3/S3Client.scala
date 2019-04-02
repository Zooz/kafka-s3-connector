package com.zooz.kafkas3connector.s3

import java.util.concurrent.{ ExecutorService, Executors }

import better.files.File
import com.amazonaws.auth.{ AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials, InstanceProfileCredentialsProvider }
import com.amazonaws.client.builder.ExecutorFactory
import com.amazonaws.event.{ ProgressEvent, ProgressListener }
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.transfer.{ TransferManager, TransferManagerBuilder, Upload }
import com.amazonaws.services.s3.{ AmazonS3, AmazonS3ClientBuilder }
import play.api.{ Configuration, Logger }
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration

/**
 * A client to all S3 operations
 *
 *  @param conf Handler to Play's configuration
 */
class S3Client(conf: Configuration) {
  private val bucket: String = conf.get[String]("s3.bucket")
  private val region: String = conf.get[String]("s3.region")
  private val endpoint: Option[String] = conf.getOptional[String]("s3._endpoint")
  private val appId: String = conf.get[String]("app.id").replace("/", "_")
  protected val logger = Logger(this.getClass())

  private val s3ClientBuilder = AmazonS3ClientBuilder
    .standard()
    .withCredentials(extractCredentials)

  private val s3Client: AmazonS3 = {
    if (endpoint.isDefined) {
      s3ClientBuilder.withEndpointConfiguration(
        new EndpointConfiguration(endpoint.get, region)).build()
    } else {
      s3ClientBuilder.withRegion(Regions.fromName(region)).build()
    }
  }

  private val transferManager: TransferManager = TransferManagerBuilder
    .standard()
    .withMultipartUploadThreshold(convertFileSizeFromMBToBytes(
      conf.get[Int]("s3.multipart_threshold_mb")))
    .withExecutorFactory(new ExecutorServiceFactory)
    .withS3Client(s3Client)
    .build()

  /**
   * Uploads a file to S3. Supports automatic multi-part upload
   *
   *  @param s3FilePath target path in S3
   *  @param local file to upload
   */
  def uploadFileToS3(s3FilePath: String, file: File): Unit = {
    try {
      val filePathWithAppId = s"$appId/$s3FilePath"
      val putObjectRequest: PutObjectRequest =
        new PutObjectRequest(bucket, filePathWithAppId, file.toJava)
      putObjectRequest.setGeneralProgressListener(createProgressListener())

      logger.info(s"uploading file = [$filePathWithAppId] to bucket = [$bucket] , file size = ${file.size} bytes") // scalastyle:ignore
      val upload: Upload = transferManager.upload(putObjectRequest)
      upload.waitForCompletion()
      logger.info(s"finish to upload [${filePathWithAppId}] to s3 ")
    } catch {
      case e: Exception =>
        logger.error(
          s"Got an exception while uploading file = $s3FilePath to bucket = [$bucket]", e)
        throw e
    }
  }

  /** Generates a progress listener used to receive notifications when bytes are transferred */
  private def createProgressListener(): ProgressListener = {
    new ProgressListener {
      override def progressChanged(progressEvent: ProgressEvent): Unit = {
        logger.debug(s"Transferred Bytes: ${progressEvent.getBytesTransferred}")
      }
    }
  }

  /**
   * Decides on a credential type - if key and secret are found in Play's configuration -
   *  uses a static (key/secret) access. Otherwise - relies on an instance profile access
   */
  private def extractCredentials: AWSCredentialsProvider = {
    val accessKey = conf.getOptional[String]("s3.access_key")
    val secretKey = conf.getOptional[String]("s3.secret_key")

    if (accessKey.isDefined && secretKey.isDefined) {
      new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey.get, secretKey.get))
    } else {
      InstanceProfileCredentialsProvider.getInstance()
    }
  }

  private def convertFileSizeFromMBToBytes(fileSize: Int): Long = {
    (fileSize * Math.pow(2, 20)).toLong
  }

  /** Creates a thread used to load a single file to S3 */
  private class ExecutorServiceFactory extends ExecutorFactory {
    override def newExecutor(): ExecutorService = {
      Executors.newFixedThreadPool(conf.get[Int]("s3.upload_threads_per_file"))
    }
  }
  
  /** Closes the transfer manager and its threadpool */
  def close() {
    transferManager.shutdownNow()
    s3Client.shutdown()
  }
}
