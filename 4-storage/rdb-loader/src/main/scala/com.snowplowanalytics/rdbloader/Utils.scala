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

// Java
import java.io.File

// json4s
import org.json4s.JValue
import org.json4s.jackson.{parseJson => parseJson4s}

// Scala
import scala.io.Source
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{ universe => ru }
import scala.util.control.NonFatal

import cats._
import cats.data._
import cats.implicits._

// Circe
import io.circe.{ HCursor, DecodingFailure, Json, Decoder, ParsingFailure }

object Utils {

  private val m = ru.runtimeMirror(getClass.getClassLoader)

  /**
   * Common trait for all ADTs that can be read from string
   * Must be extended by sealed hierarchy including only singletons
   * Used by `decodeStringEnum` to get runtime representation of whole ADT
   */
  trait StringEnum {
    /**
      * **IN** string representation.
      * It should be used only to help read `StringEnum` from string
      * and never other way round, such as render value into SQL statement
      */
    def asString: String
  }

  /**
   * Derive decoder for ADT with `StringEnum`
   *
   * @tparam A sealed hierarchy
   * @return circe decoder for ADT `A`
   */
  def decodeStringEnum[A <: StringEnum: TypeTag]: Decoder[A] =
    Decoder.instance(parseEnum[A])

  /**
   * Syntax extension to transform `Either` with string as failure
   * into circe-appropriate decoder result
   */
  implicit class ParseErrorOps[A](val error: Either[String, A]) extends AnyVal {
    def asDecodeResult(hCursor: HCursor): Decoder.Result[A] = error match {
      case Right(success) => Right(success)
      case Left(message) => Left(DecodingFailure(message, hCursor.history))
    }
  }

  /**
   * Syntax extension to parse JSON objects with known keys
   */
  implicit class JsonHashOps(val obj: Map[String, Json]) extends AnyVal {
    def getKey(key: String, hCursor: HCursor): Decoder.Result[Json] = obj.get(key) match {
      case Some(success) => Right(success)
      case None => Left(DecodingFailure(s"Key [$key] is missing", hCursor.history))
    }
  }

  /**
   * Syntax extension to transform left type in `ValidationNel`
   */
  implicit class LeftMapNel[L, R](validation: ValidatedNel[L, R]) {
    def leftMapNel[LL](l: L => LL): ValidatedNel[LL, R] =
      validation.leftMap(_.map(l))
  }

  /**
   * Read file content without throwing exceptions
   */
  def readFile(file: File): Either[ConfigError, String] =
    try {
      Source.fromFile(file).getLines.mkString("\n").asRight
    } catch {
      case NonFatal(_) => ParseError(s"File [${file.getAbsolutePath}] cannot be parsed").asLeft
    }

  /**
    * Helper method to parse Json4s JSON AST without exceptions
    *
    * @param json string presumably containing valid JSON
    * @return json4s JValue AST if success
    */
  def safeParse(json: String): Either[ConfigError, JValue] =
    try {
      parseJson4s(json).asRight
    } catch {
      case NonFatal(e) => ParseError(ParsingFailure("Invalid JSON", e)).asLeft
    }

  /**
   * Parse element of `StringEnum` sealed hierarchy from circe AST
   *
   * @param hCursor parser's cursor
   * @tparam A sealed hierarchy
   * @return either successful circe AST or decoding failure
   */
  private def parseEnum[A <: StringEnum: TypeTag](hCursor: HCursor): Decoder.Result[A] = {
    for {
      string <- hCursor.as[String]
      method  = fromString[A](string)
      result <- method.asDecodeResult(hCursor)
    } yield result
  }

  /**
   * Parse element of `StringEnum` sealed hierarchy from String
   *
   * @param string line containing `asString` representation of `StringEnum`
   * @tparam A sealed hierarchy
   * @return either successful circe AST or decoding failure
   */
  def fromString[A <: StringEnum: TypeTag](string: String): Either[String, A] = {
    val map = sealedDescendants[A].map { o => (o.asString, o) }.toMap
    map.get(string) match {
      case Some(a) => Right(a)
      case None => Left(s"Unknown ${typeOf[A].typeSymbol.name.toString} [$string]")
    }
  }

  /**
   * Get all objects extending some sealed hierarchy
   * @tparam Root some sealed trait with object descendants
   * @return whole set of objects
   */
  def sealedDescendants[Root: TypeTag]: Set[Root] = {
    val symbol = typeOf[Root].typeSymbol
    val internal = symbol.asInstanceOf[scala.reflect.internal.Symbols#Symbol]
    val descendants = if (internal.isSealed)
      Some(internal.sealedDescendants.map(_.asInstanceOf[Symbol]) - symbol)
    else None
    descendants.getOrElse(Set.empty).map(x => getCaseObject(x).asInstanceOf[Root])
  }

  /**
   * Reflection method to get runtime object by compiler's `Symbol`
   * @param desc compiler runtime `Symbol`
   * @return
   */
  private def getCaseObject(desc: Symbol): Any = {
    val mod = m.staticModule(desc.asClass.fullName)
    m.reflectModule(mod).instance
  }

  /**
   * Returns the given argument if `cond` is `true`, otherwise, unit lifted into F.
   */
  def whenA[F[_]: Applicative, A](cond: Boolean)(f: => F[A]): F[Unit] =
    if (cond) implicitly[Applicative[F]].void(f) else implicitly[Applicative[F]].pure(())

}
