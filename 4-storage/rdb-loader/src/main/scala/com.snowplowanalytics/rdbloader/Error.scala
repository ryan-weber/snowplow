package com.snowplowanalytics.rdbloader

// circe
import io.circe.{ParsingFailure, DecodingFailure}

// JSON Schema validator
import com.github.fge.jsonschema.core.report.ProcessingMessage


sealed trait ConfigError { def message: String }

case class ParseError(error: Option[ParsingFailure], textMessage: Option[String]) extends ConfigError {
  def message: String = (error, textMessage) match {
    case (_, Some(e)) => e
    case (Some(e), _) => s"Configuration parse error: ${e.toString}"
    case _ => "Unknown configuration parse error"
  }
}
case class DecodingError(decodingFailure: Option[DecodingFailure], textMessage: Option[String]) extends ConfigError {
  def message: String = (decodingFailure, textMessage) match {
    case (_, Some(e)) => e
    case (Some(e), _) => s"Configuration decode error: ${e.toString}"
    case _ => "Unknown configuration parse error"
  }
}
case class ValidationError(processingMessages: ProcessingMessage) extends ConfigError {
  def message: String = processingMessages.toString
}

object ParseError {
  def apply(message: String): ParseError =
    ParseError(None, Some(message))

  def apply(error: ParsingFailure): ParseError =
    ParseError(Some(error), None)
}

object DecodingError {
  def apply(decodingFailure: DecodingFailure): DecodingError =
    DecodingError(Some(decodingFailure), None)

  def apply(message: String): DecodingError =
    DecodingError(None, Some(message))
}

/**
 * Error representing failure on events (or types, or JSONPaths) discovery
 * These errors have aggregation semantics (as in `cats.data.Valid`)
 */
case class DiscoveryError(message: String)

/**
 * Error representing failure on database loading (or executing any statements)
 * These errors have short-circuit semantics (as in `scala.Either`)
 */
case class LoadError()

