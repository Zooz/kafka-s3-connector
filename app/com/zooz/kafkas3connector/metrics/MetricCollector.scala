package com.zooz.kafkas3connector.metrics

import io.prometheus.client.CollectorRegistry

/** A parent class to metrics to be registerd in Prometheus. Implementing classes can use the
 *  given CollectorRegistry to register their metrics
 */
abstract class MetricCollector(registry: CollectorRegistry) {

}
