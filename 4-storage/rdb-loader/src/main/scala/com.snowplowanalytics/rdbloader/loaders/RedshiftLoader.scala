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
package loaders

import cats.data._
import cats.syntax.all._
import cats.instances.all._
import Targets.RedshiftConfig
import Config.{GzipCompression, NoneCompression, OutputCompression, SnowplowAws}
import Main.{Analyze, Compupdate, Shred, Step, Vacuum}
import RefinedTypes._
import Monitoring.Tracker

object RedshiftLoader {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"

  val ALTERED_ENRICHED_PATTERN = "(run=[0-9-]+/atomic-events)".r

  case class ShreddedStatements(copy: SqlString, analyze: SqlString, vacuum: SqlString)

  def loadEventsAndShreddedTypes(config: Config, target: RedshiftConfig, steps: Set[Step]): Unit = {
    val tracker = Monitoring.initializeTracking(config.monitoring)

    val shreddedStatements = getShreddedStatements(config, target, steps)
    val atomicCopyStatements = getAtomicCopyStatements(config, target, steps)
    val manifestStatement = getManifestStatements(target.schema, shreddedStatements.size)
    val copyStatements = shreddedStatements.map(_.copy) + atomicCopyStatements + manifestStatement

    for {
      amount <- load(copyStatements, target)
      _ <- vacuum(steps.contains(Vacuum), shreddedStatements.map(_.vacuum), target)
      _ <- analyze(!steps.contains(Analyze), shreddedStatements.map(_.analyze), target)
    } yield ()


  }

  def trackIfEnabled(tracker: Option[Tracker], event: Either[Throwable, Int]): Unit = {
    tracker match {
      case Some(t) =>
        event match {
          case Right(amount) => t.trackSuccess(amount.toString)
          case Left(error) => t.trackError(error)
        }
      case None => ()
    }
  }

  def load(copyStatements: Set[SqlString], target: RedshiftConfig) =
    PostgresqlLoader.executeTransaction(target, copyStatements)

  def vacuum(execute: Boolean, shreddedVacuumStatements: Set[SqlString], target: RedshiftConfig) = {
    if (execute) {
      val vacuumStatements = shreddedVacuumStatements + buildVacuumStatement(target.getTableName)
      PostgresqlLoader.executeQueries(target, vacuumStatements).void
    } else Right(())
  }

  def analyze(execute: Boolean, shreddedAnalyzeStatements: Set[SqlString], target: RedshiftConfig) = {
    val analyzeStatements = shreddedAnalyzeStatements + buildAnalyzeStatement(target.getTableName)
    PostgresqlLoader.executeTransaction(target, analyzeStatements)
  }

  def getAtomicCopyStatements(config: Config, target: RedshiftConfig, steps: Set[Step]): SqlString = {
    if (isLegacy(config.enrich.versions.hadoopShred)) {
      ???
    } else {
      val atomicEvents = ShreddedType.findFirst(config.aws, isAtomicEvent)
      val atomicEventsRight = atomicEvents match {
        case Some(e) => e
        case None => throw new IllegalArgumentException("atomic-events is empty")
      }

      val alteredEnrichedSubdirictory = extractEnrichedSubdir(atomicEvents.get)
      buildCopyFromTsvStatement(config, atomicEventsRight, target, steps)
    }
  }

  /**
   * Remove all occurences of access key id and secret access key from message
   * Helps to avoid publishing credentials on insecure channels
   *
   * @param s original message
   * @param accessKey AWS Access Key or username
   * @param secretKey AWS Secret Access Key or password
   * @return string with hidden keys
   */
  def sanitize(s: String, accessKey: String, secretKey: String): String =
    s.replaceAll(accessKey, "x" * accessKey.length).replaceAll(secretKey, "x" * secretKey.length)

  // extracts run=xxx part
  def extractEnrichedSubdir(fullPath: String): String = ???

  def getShreddedStatements(config: Config, target: RedshiftConfig, steps: Set[Step]): Set[ShreddedStatements] = {
    if (!steps.contains(Shred)) {
      Set.empty[ShreddedStatements]
    } else {
      val shreddedTypes = ShreddedType.discoverShreddedTypes(config.aws, target.schema)
      val process = getShreddedStatement(config, target)(_)
      shreddedTypes.map(process)
    }
  }

  def getShreddedStatement(config: Config, target: RedshiftConfig)(shreddedType: Either[String, ShreddedType]): ShreddedStatements = {
    val shreddedTypeRight = shreddedType.right.toOption.get

    val jsonPathsFile =
      ShreddedType.discoverJsonPath(config.aws, shreddedTypeRight)

    val jsonPathsFileRight =
      jsonPathsFile.right.toOption.get

    val copyFromJson = buildCopyFromJsonStatement(config, shreddedTypeRight.getObjectPath, jsonPathsFileRight, target.getTableName, target.maxError)
    val analyze = buildAnalyzeStatement(target.getTableName)
    val vacuum = buildVacuumStatement(target.getTableName)

    ShreddedStatements(copyFromJson, analyze, vacuum)
  }

  def getManifestStatements(databaseSchema: String, shreddedCardinality: Int): SqlString = {
    val eventsTable = Common.getTable(databaseSchema)

    SqlString.unsafeCoerce(s"""
      |INSERT INTO $databaseSchema.manifest
      | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, $shreddedCardinality AS shredded_cardinality
      | FROM $eventsTable
      | WHERE etl_tstamp IS NOT null
      | GROUP BY 1
      | ORDER BY etl_tstamp DESC
      | LIMIT 1;""".stripMargin)
  }

  def buildAnalyzeStatement(tableName: String): SqlString = ???

  def buildVacuumStatement(tableName: String): SqlString = ???

  def buildCopyFromJsonStatement(config: Config, objectPath: String, jsonPathsFile: String, tableName: String, maxError: Int): SqlString = {
    val credentials = getCredentials(config.aws)
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)

    SqlString.unsafeCoerce(s"""
       |COPY $tableName FROM '$objectPath'
       | CREDENTIALS '$credentials' JSON AS '$jsonPathsFile'
       | REGION AS '${config.aws.s3.region}'
       | MAXERROR $maxError TRUNCATE COLUMNS TIMEFORMAT 'auto'
       | ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  def buildCopyFromTsvStatement(config: Config, s3path: String, target: RedshiftConfig, steps: Set[Step]): SqlString = {
    val credentials = getCredentials(config.aws)
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)
    val tableName = Common.getTable(target.schema)
    val comprows = if (steps.contains(Compupdate)) s"COMPUPDATE COMPROWS ${target.compRows}" else ""

    val fixedObjectPath: String = ???

    // TODO: config.enrich is incorrect!
    SqlString.unsafeCoerce(s"""
       |COPY $tableName FROM '$fixedObjectPath'
       | CREDENTIALS '$credentials' REGION AS '${config.aws.s3.region}'
       | DELIMITER '$EventFieldSeparator' MAXERROR ${target.maxError}
       | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS $comprows
       | TIMEFORMAT 'auto' ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  def getCompressionFormat(outputCodec: OutputCompression): String = outputCodec match {
    case NoneCompression => ""
    case GzipCompression => "GZIP"
  }

  def getCredentials(aws: SnowplowAws): String =
    s"aws_access_key_id=${aws.accessKeyId};aws_secret_access_key=${aws.secretAccessKey}"

  def isAtomicEvent(s3key: String): Boolean = ???

  def isLegacy(version: String): Boolean =
    version.split(".").toList match {
      case major :: "5" :: _ => true
      case _ => false
    }

}
