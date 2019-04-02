package com.zooz.kafkas3connector.modules

import com.google.inject.AbstractModule
import com.zooz.kafkas3connector.kafka.Consumer
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager
import play.api.{ Configuration, Environment }
import com.zooz.kafkas3connector.flowmanager.FlowManager
import com.zooz.kafkas3connector.buffermanagers.BuffersManager
import com.zooz.kafkas3connector.health.HealthChecker
import com.zooz.kafkas3connector.mapper.KafkaMessageMapper
import com.google.inject.name.Names
import scala.reflect.ClassTag
import play.api.libs.concurrent.AkkaGuiceSupport
import com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.S3BufferManager
import com.zooz.kafkas3connector.buffermanagers.s3buffermanagers.DefaultBufferManager
import com.zooz.kafkas3connector.health.KafkaHealthChecker
import com.zooz.kafkas3connector.mapper.NoTransformMapper
import play.api.Logger
import com.zooz.kafkas3connector.flowmanager.DeathWatcher

/** The service default loader. Binds the default buffer manager, health checker and message
 *  mappers instances and actors.
 *  Classes can extend this class in order to bind their own implementations of the above types.
 */
class DefaultLoader(
  environment:   Environment,
  configuration: Configuration)
  extends AbstractModule with AkkaGuiceSupport {

  protected val logger = Logger(this.getClass())

  /** Binds all actors and class types */
  final override def configure(): Unit = {
    logger.info(s"Loading module: ${this.getClass.getName}")
    bindActor[FlowManager]("flow-manager")
    bindActor[Consumer]("consumer")
    bindActor[OffsetsManager]("offsets-manager")

    bindBufferManager
    bindHealthChecker
    bindMessageMapper
    
    bindActor[DeathWatcher]("death-watcher")
  }

  /** Override this class to bind another sub-class of BuffersManager */
  protected def bindBufferManager: Unit = {
    bindActor[DefaultBufferManager]("buffers-manager")
  }

  /** Override this class to bind another sub-class of HealthChecker */
  protected def bindHealthChecker: Unit = {
    bindActor[KafkaHealthChecker]("health-checker")
  }

  /** Override this class to bind another sub-class of KafkaMessageMapper */
  protected def bindMessageMapper: Unit = {
    bind(classOf[KafkaMessageMapper]).to(classOf[NoTransformMapper])
  }
}
