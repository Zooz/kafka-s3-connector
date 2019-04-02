package com.zooz.kafkas3connector.modules

import play.api.inject.{Binding, Module}
import play.api.{Configuration, Environment}
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.hotspot._
import play.api.Logger
import scala.collection.Seq

/** Module used to publish all Prometheus metrics to the relevant endpoint */ 
class PrometheusModule extends Module {
  protected val logger = Logger(this.getClass())
  
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] = {
    val s3ConnectorRegistry = new CollectorRegistry()
    s3ConnectorRegistry.clear()
    new StandardExports().register(s3ConnectorRegistry)
    new MemoryPoolsExports().register(s3ConnectorRegistry)
    new BufferPoolsExports().register(s3ConnectorRegistry)
    new GarbageCollectorExports().register(s3ConnectorRegistry)
    new ThreadExports().register(s3ConnectorRegistry)
    new ClassLoadingExports().register(s3ConnectorRegistry)
    new VersionInfoExports().register(s3ConnectorRegistry)
    logger.logger.info("s3ConnectorRegistry initialized and cleared")
    Seq(
      bind[CollectorRegistry].to(s3ConnectorRegistry)
    )
  }
}
