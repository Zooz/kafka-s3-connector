package com.zooz.kafkas3connector.buffermanagers.s3buffermanagers

import javax.inject.{ Inject, Singleton }
import play.api.Configuration
import com.zooz.kafkas3connector.buffermanagers.BufferManagerMetrics
import com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.partitioners.DefaultTimePartitioner

/** A buffer manager that uses the KafkaMessage's creation to resolve the output directory's
 * date
 */
@Singleton
class DefaultBufferManager @Inject() (
  config:  Configuration,
  metrics: BufferManagerMetrics)
  extends S3BufferManager(config, metrics) with  DefaultTimePartitioner{
}
