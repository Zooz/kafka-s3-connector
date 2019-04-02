package com.zooz.kafkas3connector.buffermanagers

import javax.inject.{Inject, Singleton}
import com.zooz.kafkas3connector.metrics.MetricCollector
import io.prometheus.client.{CollectorRegistry, Gauge, Histogram}

/** A Prometheus MetricCollector class to register metrics related to buffer management 
 * 
 *  @param registry A Prometheus registry instance to which metrics are to be registered 
 */
@Singleton
class BufferManagerMetrics @Inject() (registry: CollectorRegistry)
  extends MetricCollector(registry){

  val bufferCounter: Gauge = Gauge.build()
    .name("number_of_open_buffers")
    .help("counts the number of currently open buffer").
    register(registry)

  val singleBufferFlushTimer: Histogram = Histogram.build()
    .name("single_buffer_flush_time")
    .help("measures the buffer flush time")
    .linearBuckets(1.0, 1, 25)
    .register(registry)
  
  val msgInMEMCounter: Gauge = Gauge.build()
    .name("number_of_msgs_in_mem")
    .help("counts the number of msgs in memory")
    .register(registry)
}
