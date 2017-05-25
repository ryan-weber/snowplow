/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.rdbloader

import java.io.File

import cats.data.ValidatedNel
import cats.syntax.traverse._
import cats.instances.list._
import cats.syntax.either._
import cats.syntax.validated._
import io.circe.Json
import io.circe.Decoder._
import io.circe.generic.auto._
import org.json4s.JValue
import com.github.fge.jsonschema.core.report.ProcessingMessage
import com.snowplowanalytics.rdbloader.loaders.Common

// Iglu client
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue._

import Utils._
import Compat._

object Targets {

  sealed trait SslMode extends StringEnum { def asProperty = asString.toLowerCase.replace('_', '-') }
  case object Disable extends SslMode { def asString = "DISABLE" }
  case object Require extends SslMode { def asString = "REQUIRE" }
  case object VerifyCa extends SslMode { def asString = "VERIFY_CA" }
  case object VerifyFull extends SslMode { def asString = "VERIFY_FULL" }

  sealed trait Purpose extends StringEnum
  case object DuplicateTracking extends Purpose { def asString = "DUPLICATE_TRACKING" }
  case object FailedEvents extends Purpose { def asString = "FAILED_EVENTS" }
  case object EnrichedEvents extends Purpose { def asString = "ENRICHED_EVENTS" }

  implicit val sslModeDecoder =
    decodeStringEnum[SslMode]

  implicit val purposeDecoder =
    decodeStringEnum[Purpose]

  sealed trait StorageTarget extends Product with Serializable {
    def name: String
    def purpose: Purpose
  }

  case class ElasticConfig(
      name: String,
      host: String,
      index: String,
      port: Int,
      `type`: String,
      nodesWanOnly: Boolean)
    extends StorageTarget {
    val purpose = FailedEvents
  }

  case class AmazonDynamodbConfig(
      name: String,
      accessKeyId: String,
      secretAccessKey: String,
      awsRegion: String,
      dynamodbTable: String)
    extends StorageTarget {
    val purpose = DuplicateTracking
  }

  case class PostgresqlConfig(
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: String)
    extends StorageTarget with JdbcConfig {
    val purpose = EnrichedEvents
  }

  case class RedshiftConfig(
      name: String,
      host: String,
      database: String,
      port: Int,
      sslMode: SslMode,
      schema: String,
      username: String,
      password: String,
      maxError: Int,
      compRows: Long)
    extends StorageTarget with JdbcConfig {
    val purpose = EnrichedEvents
  }

  /**
   * Common configuration for JDBC target, such as Redshift and Postgres
   * Any of those can be safely coerced
   */
  trait JdbcConfig {
    def name: String
    def host: String
    def database: String
    def schema: String
    def port: Int
    def sslMode: SslMode
    def username: String
    def password: String

    def getTableName = Common.getTable(schema)
  }


  /**
    * Read all JSONs in directory, validate via Iglu as available storage targets
    * and extract case classes.
    *
    * @param directory file object pointing to directory with configuration JSONs
    * @param resolver Iglu resolver object
    * @return list of validated storage targets OR
    *         non-empty list of errors if any error occurred during processing of any file
    */
  def loadTargetsFromDir(directory: File, resolver: Resolver): ValidatedNel[ConfigError, List[Targets.StorageTarget]] = {
    if (!directory.isDirectory) ParseError(s"[${directory.getAbsolutePath}] is not a directory").invalidNel
    else if (!directory.canRead) ParseError(s"Targets directory [${directory.getAbsolutePath} is not readable").invalidNel
    else {
      val fileList = directory.listFiles.toList
      val targetsList = fileList.map(Targets.loadFromFile(resolver))
      targetsList.sequenceU
    }
  }

  /**
    * Decode Json as one of known storage targets
    *
    * @param validJson JSON that is presumably self-describing storage target configuration
    * @return validated entity of `StorageTarget` ADT if success
    */
  def decodeStorageTarget(validJson: Json): Either[DecodingError, StorageTarget] = {
    val nameDataPair = for {
      jsonObject <- validJson.asObject
      schema     <- jsonObject.toMap.get("schema")
      data       <- jsonObject.toMap.get("data")
      schemaKey  <- schema.asString
      key        <- SchemaKey.fromUri(schemaKey)
    } yield (key.name, data)

    nameDataPair match {
      case Some(("elastic_config", data)) => data.as[ElasticConfig].leftMap(DecodingError.apply)
      case Some(("amazon_dynamodb_config", data)) => data.as[AmazonDynamodbConfig].leftMap(DecodingError.apply)
      case Some(("postgresql_config", data)) => data.as[PostgresqlConfig].leftMap(DecodingError.apply)
      case Some(("redshift_config", data)) => data.as[RedshiftConfig].leftMap(DecodingError.apply)
      case Some((name, _)) => DecodingError(s"Unknown storage target [$name]").asLeft
      case None => DecodingError("Not a self-describing JSON was used as storage target configuration").asLeft
    }
  }

  import cats.data._


  /**
    * Parse string as `StorageTarget` validating it via Iglu resolver
    *
    * @param resolver Iglu resolver
    * @param target string presumably containing self-describing JSON with storage target
    * @return valid `StorageTarget` OR
    *         non-empty list of errors (such as validation or parse errors)
    */
  def parseTarget(resolver: Resolver)(target: String): ValidatedNel[ConfigError, StorageTarget] = {
    val json = safeParse(target).toValidatedNel
    val validatedJson = json.andThen(validate(resolver))
    validatedJson.andThen(decodeStorageTarget(_).toValidatedNel)
  }

  /**
    * Load some `StorageTarget` from JSON file
    *
    * @param resolver Iglu resolver object
    * @param file java file object with presumably JSON
    * @return
    */
  private def loadFromFile(resolver: Resolver)(file: File): ValidatedNel[ConfigError, StorageTarget] = {
    val parseAndValidate = parseTarget(resolver)(_)
    val content = readFile(file).toValidatedNel
    content.andThen(parseAndValidate)
  }

  /**
    * Validate json4s JValue AST with Iglu Resolver and immediately convert it into circe AST
    *
    * @param resolver Iglu Resolver object
    * @param json json4s AST
    * @return circe AST
    */
  private def validate(resolver: Resolver)(json: JValue): ValidatedNel[ConfigError, Json] = {
    val result: ValidatedNel[ProcessingMessage, JValue] = json.validate(dataOnly = false)(resolver)
    result.map(Compat.jvalueToCirce).leftMapNel(ValidationError)  // Convert from Iglu client's format, TODO compat
  }
}
