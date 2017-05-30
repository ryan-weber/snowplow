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

import java.io.FileReader
import java.nio.file._
import java.sql.{SQLException, SQLTimeoutException}
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.convert.wrapAsScala._

import cats.implicits._

import org.postgresql.Driver
import org.postgresql.jdbc.PgConnection
import org.postgresql.copy.CopyManager

import Targets.{JdbcConfig, PostgresqlConfig}
import Main.{Analyze, Step, Vacuum}
import RefinedTypes.SqlString
import Config.Monitoring


object PostgresqlLoader {

  val EventFiles = "part-*"
  val EventFieldSeparator = "\t"
  val NullString = ""
  val QuoteChar = "\\x01"
  val EscapeChar = "\\x02"

  case class PostgresLoadError(file: Path, exception: Exception)
  case class PostgresQueryError(query: String, exception: Exception)

  /**
    *
    * @param folder local folder with downloaded events
    * @param target
    * @param monitoring
    */
  def loadEvents(folder: String, target: PostgresqlConfig, steps: Set[Step], monitoring: Monitoring): Unit = {
    val eventFiles = getEventFiles(Paths.get(folder))
    val copyResult = copyViaStdin(target, eventFiles)
    val tracker = LoadTracker.initializeTracking(monitoring)

    copyResult match {
      case Left(_) =>
        ???
      case Right(_) =>
        getPostprocessingStatement(target, steps) match {
          case Some(statement) =>
            // TODO: this should be handled
            executeQueries(target, List(statement)).map(_.toLong)
          case None => ()
        }
    }
  }


  def executeTransaction(config: JdbcConfig, queries: List[SqlString]): Either[PostgresQueryError, Unit] = {
    val begin = SqlString.unsafeCoerce("BEGIN;")
    val commit = SqlString.unsafeCoerce("COMMIT;")
    val transaction = (begin :: queries) :+ commit
    executeQueries(config, transaction).void
  }

  /**
    * Without trailing space
    * @param steps
    * @return
    */
  def getPostprocessingStatement(target: PostgresqlConfig, steps: Set[Step]): Option[SqlString] = {
    val statements = List(steps.find(_ == Vacuum), steps.find(_ == Analyze)).flatten.map(_.asString.toUpperCase)
    statements match {
      case Nil => None
      case steps => Some(SqlString.unsafeCoerce(s"${steps.mkString(" ")} ${Common.getEventsTable(target.schema)};"))
    }
  }

  /**
   * Create new connection and execute set of SQL update-statements inside it,
   * close connection afterwards
   *
   * @param target configuration for JDBC target
   * @param queries set of valid SQL statements in string representation
   * @return number of updated rows in case of success, first failure otherwise
   */
  def executeQueries(target: JdbcConfig, queries: List[SqlString]): Either[PostgresQueryError, Int] = {
    val conn = getConnection(target)
    val result = queries.traverse(executeQuery(conn)).map(_.combineAll)
    conn.close()
    result
  }

  /**
   * Execute a single update-statement in provided Postgres connection
   *
   * @param connection Postgres connection
   * @param sql string with valid SQL statement
   * @return number of updated rows in case of success, failure otherwise
   */
  def executeQuery(connection: PgConnection)(sql: String): Either[PostgresQueryError, Int] = {
    try {
      connection.createStatement().executeUpdate(sql).asRight
    } catch {
      case e: SQLException => PostgresQueryError(sql, e).asLeft
      case e: SQLTimeoutException => PostgresQueryError(sql, e).asLeft
    }
  }

  def atomicEventsMatcher(root: Path): PathMatcher =
    FileSystems.getDefault.getPathMatcher(s"glob:${root.toAbsolutePath.toString}/*/atomic-events/part-*");

  /**
    * It will match all atomic-event part-files shredded archive
    *
    * @param root
    * @return
    */
  def getEventFiles(root: Path): List[Path] = {
    val matcher = atomicEventsMatcher(root)
    def go(folder: Path, deep: Int = 2): List[Path] =
      Files.newDirectoryStream(folder).toList.flatMap { path =>
        if (Files.isDirectory(path) && deep > 0) go(path, deep - 1)
        else if (matcher.matches(path.toAbsolutePath)) List(path)
        else Nil
      }

    go(root)
  }

  def copyViaStdin(target: PostgresqlConfig, files: List[Path]): Either[PostgresLoadError, Long] = {
    val eventsTable = Common.getEventsTable(target.schema)
    val copyStatement = s"COPY $eventsTable FROM STDIN WITH CSV ESCAPE E'$EscapeChar' QUOTE E'$QuoteChar' DELIMITER '$EventFieldSeparator' NULL '$NullString'"

    val conn = getConnection(target)
    conn.setAutoCommit(false)
    val copyManager = new CopyManager(conn)
    val result = files.traverse(copyIn(copyManager, copyStatement)).map(_.combineAll)
    if (result.isLeft) conn.rollback() else conn.commit()
    conn.close()
    result
  }

  def copyIn(copyManager: CopyManager, copyStatement: String)(file: Path): Either[PostgresLoadError, Long] = {
    try {
      copyManager.copyIn(copyStatement, new FileReader(file.toFile)).asRight
    } catch {
      case e: SQLException => PostgresLoadError(file, e).asLeft
      case e: SQLTimeoutException => PostgresLoadError(file, e).asLeft
    }
  }


  /**
    * Copy list of files into Redshift.
    * Short-circuit on first `Left` value of `process`
    *
    * @param list
    * @return total number of loaded rows in case of success
    *         or first error in case of failure
    */
  def copyInFiles(list: List[String], process: String => Either[PostgresLoadError, Long]): Either[PostgresLoadError, Long] = {
    @tailrec def go(files: List[String], itemsLoaded: Long): Either[PostgresLoadError, Long] = files match {
      case Nil => itemsLoaded.asRight
      case file :: remain => process(file) match {
        case Right(count) => go(remain, count + itemsLoaded)
        case Left(error) => error.asLeft
      }
    }

    go(list, 0L)
  }

  // Common for Redshift and PostgreSQL


  def getConnection(target: JdbcConfig): PgConnection = {
    val url = s"jdbc:postgresql://${target.host}:${target.port}/${target.database}"

    val props = new Properties()
    props.setProperty("user", target.username)
    props.setProperty("password", target.password)
    props.setProperty("sslmode", target.sslMode.asProperty)
    props.setProperty("tcpKeepAlive", "true")

    new Driver().connect(url, props).asInstanceOf[PgConnection]
  }
}
