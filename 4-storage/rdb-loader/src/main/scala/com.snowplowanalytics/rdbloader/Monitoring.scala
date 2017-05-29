package com.snowplowanalytics.rdbloader

// Json4s
import org.json4s.JObject

// Snowplow Tracker
import com.snowplowanalytics.snowplow.scalatracker._
import com.snowplowanalytics.snowplow.scalatracker.emitters.{AsyncBatchEmitter, AsyncEmitter}

// This project
import Config._


trait LoadTracker {
  def trackError(error: String): Unit
  def trackSuccess(message: String): Unit
}

object LoadTracker {

  val ApplicationContextSchema = "iglu:com.snowplowanalytics.monitoring.batch/application_context/jsonschema/1-0-0"
  val LoadSucceededSchema = "iglu:com.snowplowanalytics.monitoring.batch/load_succeeded/jsonschema/1-0-0"
  val LoadFailedSchema = "iglu:com.snowplowanalytics.monitoring.batch/load_failed/jsonschema/1-0-0"

  class SnowplowTracker(tracker: Tracker) extends LoadTracker {
    def trackError(error: String): Unit = {
      tracker.trackUnstructEvent(SelfDescribingJson(
        LoadFailedSchema,
        JObject(Nil)
      ))
    }

    def trackSuccess(message: String): Unit = {
      tracker.trackUnstructEvent(SelfDescribingJson(
        LoadSucceededSchema,
        JObject(Nil)
      ))
    }
  }

  object Collector {
    def isInt(s: String): Boolean = try { s.toInt; true } catch { case _: NumberFormatException => false }

    def unapply(hostPort: String): Option[(String, Int)] =
      hostPort.split(":").toList match {
        case host :: port :: Nil if isInt(port) => Some((host, port.toInt))
        case host :: Nil => Some((host, 80))
        case _ => None
      }
  }

  def initializeTracking(monitoring: Monitoring): Option[LoadTracker] = {
    monitoring.snowplow.collector match {
      case Some(Collector((host, port))) =>
        val emitter = monitoring.snowplow.method match {
          case GetMethod =>
            AsyncEmitter.createAndStart(host, port = port)
          case PostMethod =>
            AsyncBatchEmitter.createAndStart(host, port = port, bufferSize = 2)
        }
        val tracker = new Tracker(List(emitter), "snowplow-rdb-loader", monitoring.snowplow.appId)
        Some(new SnowplowTracker(tracker))
      case Some(invalidHost) =>
        println(s"Invalid Collector Host [$invalidHost]")
        None
      case None =>
        None
    }
  }

  /**
   * Log loading result via Snowplow trackier if tracking is enabled
   *
   * @param result either failure or success of loading
   * @param tracker Snowplow tracker
   * @return same loading result
   */
  def log[E, A](result: Either[E, A], tracker: Option[LoadTracker]): Either[E, A] = {
    result match {
      case Left(error) => tracker.foreach(_.trackError(error.toString))
      case Right(_) => tracker.foreach(_.trackSuccess(""))
    }
    result
  }
}
