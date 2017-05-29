/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
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

// cats
import cats.implicits._

// This project
import Main.{Analyze, Compupdate, Shred, Step, Vacuum}
import PostgresqlLoader.{ executeTransaction, executeQueries }
import RefinedTypes._
import Targets.RedshiftConfig
import Utils.whenA
import LoadTracker.log

object RedshiftLoader {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"

  /**
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  case class ShreddedStatements(copy: SqlString, analyze: SqlString, vacuum: SqlString)


  def loadEventsAndShreddedTypes(config: Config, target: RedshiftConfig, steps: Set[Step]) = {
    val tracker = LoadTracker.initializeTracking(config.monitoring)

    for {
      atomicCopyStatements <- getAtomicCopyStatements(config, target, steps)
      shreddedStatements   <- getShreddedStatements(config, target, steps).map(_.toSet)
      manifestStatement = getManifestStatements(target.schema, shreddedStatements.size)
      copyStatements = (shreddedStatements.map(_.copy) + atomicCopyStatements + manifestStatement).toList

      vacuumStatements = (shreddedStatements.map(_.vacuum) + buildVacuumStatement(target.eventsTable)).toList
      analyzeStatements = (shreddedStatements.map(_.analyze) + buildAnalyzeStatement(target.eventsTable)).toList

      _ <- log(executeTransaction(target, copyStatements), tracker)
      _ <- whenA(steps.contains(Vacuum))(executeQueries(target, vacuumStatements))
      _ <- whenA(steps.contains(Analyze))(executeTransaction(target, analyzeStatements))

    } yield ()
  }

  case class TempThrowable(val message: String) extends Throwable

  /**
   *
   *
   * @param config
   * @param target
   * @param steps
   * @return
   */
  def getAtomicCopyStatements(config: Config, target: RedshiftConfig, steps: Set[Step]): Either[TempThrowable, SqlString] = {
    S3.findAtomicEventsKey(config.aws) match {
      case Some(atomicEventsKeyPath) =>
        Right(buildCopyFromTsvStatement(config, atomicEventsKeyPath, target, steps))
      case None =>
        Left(TempThrowable(s"Cannot find atomic-events directory in shredded/good [${config.aws.s3.buckets.shredded.good}]"))
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

  /**
   *
   *
   * @param config
   * @param target
   * @param steps
   * @return
   */
  def getShreddedStatements(config: Config, target: RedshiftConfig, steps: Set[Step]): Either[String, List[ShreddedStatements]] = {
    if (!steps.contains(Shred)) {
      Right(List.empty[ShreddedStatements])
    } else {
      val shreddedTypes = ShreddedType.discoverShreddedTypes(config.aws).toList.sequence
      val process = getShreddedStatement(config, target)(_)
      for {
        types <- shreddedTypes
        res <- types.traverse(process)
      } yield res
    }
  }

  def getShreddedStatement(config: Config, target: RedshiftConfig)(shreddedType: ShreddedType): Either[String, ShreddedStatements] = {
    val jsonPathsFile = ShreddedType.discoverJsonPath(config.aws, shreddedType)

    jsonPathsFile.map { jp =>
      val tableName = ShreddedType.getTable(shreddedType, target.schema)
      val copyFromJson = buildCopyFromJsonStatement(config, shreddedType.getObjectPath, jp, tableName, target.maxError)
      val analyze = buildAnalyzeStatement(target.eventsTable)
      val vacuum = buildVacuumStatement(target.eventsTable)

      ShreddedStatements(copyFromJson, analyze, vacuum)
    }
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

  def buildAnalyzeStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"ANALYZE $tableName;")

  def buildVacuumStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"VACUUM SORT ONLY $tableName;")

  /**
   * Build COPY FROM JSON SQL-statement for shredded types
   * 
   * @param config main Snowplow configuration
   * @param s3path S3 path to folder with shredded JSON files
   * @param jsonPathsFile S3 path to JSONPath file
   * @param tableName valid Redshift table name for shredded type
   * @return valid SQL statement to LOAD
   */
  def buildCopyFromJsonStatement(config: Config, s3path: String, jsonPathsFile: String, tableName: String, maxError: Int): SqlString = {
    val credentials = getCredentials(config.aws)
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)

    SqlString.unsafeCoerce(s"""
       |COPY $tableName FROM '$s3path'
       | CREDENTIALS '$credentials' JSON AS '$jsonPathsFile'
       | REGION AS '${config.aws.s3.region}'
       | MAXERROR $maxError TRUNCATE COLUMNS TIMEFORMAT 'auto'
       | ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  /**
   * Build COPY FROM TSV SQL-statement for non-shredded types and atomic.events table
   *
   * @param config main Snowplow configuration
   * @param s3path S3 path to atomic-events folder with shredded TSV files
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @return valid SQL statement to LOAD
   */
  def buildCopyFromTsvStatement(config: Config, s3path: AtomicEventsKey, target: RedshiftConfig, steps: Set[Step]): SqlString = {
    val credentials = getCredentials(config.aws)
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)
    val comprows = if (steps.contains(Compupdate)) s"COMPUPDATE COMPROWS ${target.compRows}" else ""
    target.eventsTable

    SqlString.unsafeCoerce(s"""
       |COPY ${target.eventsTable} FROM '$s3path'
       | CREDENTIALS '$credentials' REGION AS '${config.aws.s3.region}'
       | DELIMITER '$EventFieldSeparator' MAXERROR ${target.maxError}
       | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS $comprows
       | TIMEFORMAT 'auto' ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  /**
   * Stringify output codec to use in SQL statement
   */
  private def getCompressionFormat(outputCodec: Config.OutputCompression): String = outputCodec match {
    case Config.NoneCompression => ""
    case Config.GzipCompression => "GZIP"
  }

  def getCredentials(aws: Config.SnowplowAws): String =
    s"aws_access_key_id=${aws.accessKeyId};aws_secret_access_key=${aws.secretAccessKey}"

}
