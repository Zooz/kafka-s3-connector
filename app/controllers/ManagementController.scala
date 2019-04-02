package controllers

import com.zooz.kafkas3connector.metrics.WriterAdapter
import com.zooz.kafkas3connector.health.HealthChecker
import com.zooz.kafkas3connector.health.HealthChecker.{ HealthResult, RunAllChecks }
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import javax.inject.Inject
import play.api.mvc.{ AbstractController, ControllerComponents }
import akka.actor.ActorRef
import javax.inject.Named
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.ExecutionContext

/** Exposing the services HTTP endpoints */
class ManagementController @Inject() (
  registry:                                CollectorRegistry,
  controllerComponents:                    ControllerComponents,
  @Named("health-checker") healthChecker:  ActorRef)(implicit executionContext: ExecutionContext)
  extends AbstractController(controllerComponents) {
  implicit val timeout: Timeout = 5.seconds

  /** Called by the "GET /health" route. Calls the main HealthChecker actor and asks for its
   *  health result
   */
  def isHealthy = Action.async {
    (healthChecker ? RunAllChecks).mapTo[HealthResult].map {
      healthStatus =>
        if (healthStatus.isSuccessful) {
          Ok(healthStatus.details)
        } else {
          BadGateway(healthStatus.details)
        }
    }.recover {
      case e: scala.concurrent.TimeoutException =>
        InternalServerError("timeout")
    }
  }

  /** Called by the "GET /metrics" route. Used by Prometheus to scrape its metrics */
  def fetchMetrics() = Action { request =>
    {
      val samples = new StringBuilder()
      val writer = new WriterAdapter(samples)
      TextFormat.write004(writer, registry.metricFamilySamples())
      writer.close()
      Ok(samples.toString())
    }
  }
}
