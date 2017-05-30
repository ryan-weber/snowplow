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
   * Discovery data in `shredded.good` and associated metadata (types, JSONPaths etc),
   * build SQL statements to load this data and perform loading.
   * Primary working method
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   */
  def loadEventsAndShreddedTypes(config: Config, target: RedshiftConfig, steps: Set[Step]) = {
    val tracker = LoadTracker.initializeTracking(config.monitoring)

    for {
      // Discovering event types and building SQL-statements
      atomicCopyStatements <- getAtomicCopyStatements(config, target, steps)
      shreddedStatements   <- ShreddedStatements.discover(config, target, steps).map(_.toSet)
      manifestStatement     = getManifestStatements(target.schema, shreddedStatements.size)
      copyStatements        = (shreddedStatements.map(_.copy) + atomicCopyStatements + manifestStatement).toList

      vacuumStatements  = (shreddedStatements.map(_.vacuum) + buildVacuumStatement(target.eventsTable)).toList
      analyzeStatements = (shreddedStatements.map(_.analyze) + buildAnalyzeStatement(target.eventsTable)).toList

      // Executing SQL-statements
      _ <- log(executeTransaction(target, copyStatements), tracker)
      _ <- whenA(steps.contains(Vacuum))(executeQueries(target, vacuumStatements))
      _ <- whenA(steps.contains(Analyze))(executeTransaction(target, analyzeStatements))

    } yield ()
  }

  /**
   * Discovers data in `shredded.good` folder with its associated metadata
   * (types, JSONPath files etc) and build SQL-statements to load it
   *
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @return
   */
  // TODO: refactor load function using this
  def discover(config: Config, target: RedshiftConfig, steps: Set[Step]) = {

    for {
    // Discovering event types and building SQL-statements
      atomicCopyStatements <- getAtomicCopyStatements(config, target, steps)
      shreddedStatements <- ShreddedStatements.discover(config, target, steps).map(_.toSet)
      manifestStatement = getManifestStatements(target.schema, shreddedStatements.size)
      copyStatements = (shreddedStatements.map(_.copy) + atomicCopyStatements + manifestStatement).toList

      vacuumStatements = (shreddedStatements.map(_.vacuum) + buildVacuumStatement(target.eventsTable)).toList
      analyzeStatements = (shreddedStatements.map(_.analyze) + buildAnalyzeStatement(target.eventsTable)).toList

    } yield ()
  }

  /**
   * Find remaining run id in `shredded/good` folder and build SQL statement to
   * COPY FROM this run id
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param steps SQL steps
   * @return valid SQL statement to LOAD
   */
  def getAtomicCopyStatements(config: Config, target: RedshiftConfig, steps: Set[Step]): Either[DiscoveryError, SqlString] = {
    S3.findAtomicEventsKey(config.aws) match {
      case Some(atomicEventsKeyPath) =>
        val copyStatement = buildCopyFromTsvStatement(config, target, atomicEventsKeyPath, steps)
        Right(copyStatement)
      case None =>
        Left(DiscoveryError(s"Cannot find atomic-events directory in shredded/good [${config.aws.s3.buckets.shredded.good}]"))
    }
  }

  /**
   * Build COPY FROM TSV SQL-statement for non-shredded types and atomic.events table
   *
   * @param config main Snowplow configuration
   * @param target Redshift storage target configuration
   * @param s3path S3 path to atomic-events folder with shredded TSV files
   * @param steps SQL steps
   * @return valid SQL statement to LOAD
   */
  def buildCopyFromTsvStatement(config: Config, target: RedshiftConfig, s3path: S3Bucket, steps: Set[Step]): SqlString = {
    val credentials = getCredentials(config.aws)
    val compressionFormat = getCompressionFormat(config.enrich.outputCompression)
    val comprows = if (steps.contains(Compupdate)) s"COMPUPDATE COMPROWS ${target.compRows}" else ""

    SqlString.unsafeCoerce(s"""
      |COPY ${target.eventsTable} FROM '$s3path'
      | CREDENTIALS '$credentials' REGION AS '${config.aws.s3.region}'
      | DELIMITER '$EventFieldSeparator' MAXERROR ${target.maxError}
      | EMPTYASNULL FILLRECORD TRUNCATECOLUMNS $comprows
      | TIMEFORMAT 'auto' ACCEPTINVCHARS $compressionFormat;""".stripMargin)
  }

  /**
   * Build standard manifest-table insertion
   *
   * @param databaseSchema storage target schema
   * @param shreddedCardinality
   * @return
   */
  def getManifestStatements(databaseSchema: String, shreddedCardinality: Int): SqlString = {
    val eventsTable = Common.getEventsTable(databaseSchema)

    SqlString.unsafeCoerce(s"""
      |INSERT INTO ${Common.getManifestTable(databaseSchema)}
      | SELECT etl_tstamp, sysdate AS commit_tstamp, count(*) AS event_count, $shreddedCardinality AS shredded_cardinality
      | FROM $eventsTable
      | WHERE etl_tstamp IS NOT null
      | GROUP BY 1
      | ORDER BY etl_tstamp DESC
      | LIMIT 1;""".stripMargin)
  }

  /**
   * Build ANALYZE SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid ANALYZE SQL-statement
   */
  def buildAnalyzeStatement(tableName: String): SqlString =
    SqlString.unsafeCoerce(s"ANALYZE $tableName;")

  /**
   * Build VACUUM SQL-statement
   *
   * @param tableName full (with schema) table name for main or shredded type table
   * @return valid ANALYZE SQL-statement
   */
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
   * SQL statements for particular shredded type, grouped by their purpose
   *
   * @param copy main COPY FROM statement to load shredded type in its dedicate table
   * @param analyze ANALYZE SQL-statement for dedicated table
   * @param vacuum VACUUM SQL-statement for dedicate table
   */
  private case class ShreddedStatements(copy: SqlString, analyze: SqlString, vacuum: SqlString)

  private object ShreddedStatements {

    /**
     * Build groups of SQL statement for all shredded types
     * It discovers all shredded types in `shredded/good` (if `shred` step is not omitted),
     *
     * @param config main Snowplow configuration
     * @param target Redshift storage target configuration
     * @param steps SQL steps
     * @return either list of
     */
    def discover(config: Config, target: RedshiftConfig, steps: Set[Step]): Either[DiscoveryError, List[ShreddedStatements]] = {
      // TODO: this should have error-aggregation semantics
      if (!steps.contains(Shred)) {
        Right(List.empty[ShreddedStatements])
      } else {
        val shredJobVersion = config.storage.versions.relationalDatabaseShredder
        val shreddedTypes = ShreddedType.discoverShreddedTypes(config.aws, shredJobVersion).toList.sequence
        val process = getShreddedStatement(config, target)(_)
        for {
          types <- shreddedTypes
          res <- types.traverse(process)
        } yield res
      }
    }

    /**
     * Build group of SQL statements for particular shredded type
     *
     * @param config main Snowplow configuration
     * @param target Redshift storage target configuration
     * @param shreddedType information about shredded type found in `shredded/good`
     * @param jsonPaths existing JSONPaths S3 path for `shreddedType`
     * @return three SQL-statements to load `shreddedType` from S3
     */
    def transformShreddedType(config: Config, target: RedshiftConfig, shreddedType: ShreddedType)(jsonPaths: String): ShreddedStatements = {
      val tableName = ShreddedType.getTable(shreddedType, target.schema)
      val copyFromJson = buildCopyFromJsonStatement(config, shreddedType.getObjectPath, jsonPaths, tableName, target.maxError)
      val analyze = buildAnalyzeStatement(tableName)
      val vacuum = buildVacuumStatement(tableName)
      ShreddedStatements(copyFromJson, analyze, vacuum)
    }

    /**
     * Discover a JSONPaths file for shredded type on S3
     * and build group of SQL statements (such as COPY, ANALYZE, VACUUM) for it
     *
     * @param config main Snowplow configuration
     * @param target Redshift storage target configuration
     * @param shreddedType information about shredded type found in `shredded/good`
     * @return group of SQL-statement to load
     */
    def getShreddedStatement(config: Config, target: RedshiftConfig)(shreddedType: ShreddedType): Either[DiscoveryError, ShreddedStatements] = {
      val jsonPathsFile = ShreddedType.discoverJsonPath(config.aws, shreddedType)
      val buildShreddedStatement = transformShreddedType(config, target, shreddedType)(_)
      jsonPathsFile.map(buildShreddedStatement)
    }
  }

  /**
   * Stringify output codec to use in SQL statement
   */
  private def getCompressionFormat(outputCodec: Config.OutputCompression): String = outputCodec match {
    case Config.NoneCompression => ""
    case Config.GzipCompression => "GZIP"
  }

  /**
   * Build valid CREDENTIALS string to be used in COPY FROM SQL-statements
   *
   * @param aws Snowplow AWS configuration
   * @return string with encoded credentials
   */
  private def getCredentials(aws: Config.SnowplowAws): String =
    s"aws_access_key_id=${aws.accessKeyId};aws_secret_access_key=${aws.secretAccessKey}"
}
