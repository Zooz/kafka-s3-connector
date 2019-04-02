package controllers

import javax.inject.{ Inject, Singleton, Named }
import play.api.mvc.ControllerComponents
import play.api.mvc.AbstractController
import play.api.mvc.Request
import play.api.mvc.AnyContent
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.ExecutionContext
import akka.actor.ActorRef
import com.zooz.kafkas3connector.kafka.offsets.OffsetsManager
import com.zooz.kafkas3connector.flowmanager.FlowManager
import org.joda.time.format.DateTimeFormat
import scala.concurrent.Future
import org.joda.time.DateTime
import play.api.mvc.Result

/**
 * This controller creates the service's front page
 */
@Singleton
class FrontPageController @Inject() (
  cc:                                       ControllerComponents,
  conf:                                     play.api.Configuration,
  template:                                 views.html.index,
  @Named("offsets-manager") offsetsManager: ActorRef,
  @Named("flow-manager") flowManager:       ActorRef)(implicit executionContext: ExecutionContext)
  extends AbstractController(cc) {
  val PARTITIONS_LIMIT = 20 // Maximum partitions to display their stats in the UI

  val dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  implicit val timeout: Timeout = 30.seconds

  /** Called by the "GET /" route */
  def index = Action.async { request =>
    {
      val flushIntervalMs: Long =
        conf.getOptional[Long]("app.flush_interval_ms").getOrElse(
          conf.get[Long]("app.flush_interval_min") * 60L * 1000L)
      val flushIntervalSecs: Int = (flushIntervalMs / 1000).toInt

      // Getting first 20 partitions and their offsets data from the OffsetsManager actor
      val partitionStatusFuture: Future[OffsetsManager.PartitionsStatus] =
        (offsetsManager ? OffsetsManager.ReportPartitionsStatus(PARTITIONS_LIMIT))
          .mapTo[OffsetsManager.PartitionsStatus]

      // Getting the last flush time from the FlowManager actor. Calculating the next flush
      // time and converting both dates into strings to be embedded in response
      val lastFlushTimeFuture: Future[DateTime] = (flowManager ? FlowManager.QueryLastFlushTime)
        .mapTo[FlowManager.LastFlushTime].map(_.flushTime)
      val nextFlushTimeFuture: Future[DateTime] = lastFlushTimeFuture.map(
          _.plusSeconds(flushIntervalSecs))
      val lastFlushTimeStrFuture: Future[String] = lastFlushTimeFuture.map(dateFormatter.print(_))
      val nextFlushTimeStrFuture: Future[String] = nextFlushTimeFuture.map(dateFormatter.print(_))

      // Passing the results to the Twirl template
      val mvcResultFuture: Future[Result] = for {
        lastFlushTimeStr <- lastFlushTimeStrFuture
        nextFlushTimeStr <- nextFlushTimeStrFuture
        partitionStatus <- partitionStatusFuture
      } yield (Ok(template(lastFlushTimeStr, nextFlushTimeStr, partitionStatus.partitions)))

      mvcResultFuture.recover {
        case e: scala.concurrent.TimeoutException =>
          InternalServerError("timeout")
      }
    }
  }
}
