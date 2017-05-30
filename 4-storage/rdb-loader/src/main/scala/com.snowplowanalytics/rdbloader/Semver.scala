/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

// cats
import cats.{ PartialOrder, Show }
import cats.implicits._

// circe
import io.circe.Decoder

// This project
import Utils.IntString

/**
 * Snowplow-specific semantic versioning for apps, libs and jobs,
 * with defined decoders and ordering to compare different versions
 */
case class Semver(major: Int, minor: Int, patch: Int, prerelease: Option[Semver.Prerelease])

/**
 * Helpers for Snowplow-specific semantic versioning
 */
object Semver {

  /**
   * Prerelease part of semantic version, mostly intended to be private
   * Milestone is for libs, release candidate for apps,
   * unknown as last resort - will match any string
   */
  sealed trait Prerelease { def full: String }
  case class Milestone(version: Int) extends Prerelease { def full = s"-M$version"}
  case class ReleaseCandidate(version: Int) extends Prerelease { def full = s"-rc$version"}
  case class Unknown(full: String) extends Prerelease


  val semverPattern = """^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.([0-9]*)(.*)$""".r
  val milestonePattern = """-M([1-9][0-9]*)$"""".r
  val rcPattern = """-rc([1-9][0-9]*)$"""".r

  /**
   * Alternative constructor to omit optional prerelease part
   */
  def apply(major: Int, minor: Int, patch: Int): Semver =
    Semver(major, minor, patch, None)

  /**
   * Partial ordering instance for optional `Prerelease`.
   * Can compare only same milestones with milestones, rcs with rcs
   * and final release with prerelease
   */
  private implicit val prereleaseOrder = new PartialOrder[Option[Prerelease]] {
    def partialCompare(x: Option[Prerelease], y: Option[Prerelease]): Double = (x, y) match {
      case (Some(_), None) => -1
      case (None, Some(_)) => 1
      case (Some(Milestone(xm)), Some(Milestone(ym))) =>
        xm.partialCompare(ym)
      case (Some(ReleaseCandidate(xrc)), Some(ReleaseCandidate(yrc))) =>
        xrc.partialCompare(yrc)
      case _ => Double.NaN
    }
  }

  /**
   * Partial ordering instance for semantic version
   * Order isn't defined for prereleases of different types,
   * prerelease is always "less" than final release
   */
  implicit val semverOrder = new PartialOrder[Semver] {
    def partialCompare(x: Semver, y: Semver): Double = {
      implicitly[PartialOrder[(Int, Int, Int, Option[Prerelease])]].partialCompare(
        (x.major, x.minor, x.patch, x.prerelease),
        (y.major, y.minor, y.patch, y.prerelease)
      )
    }
  }

  /**
   * Decode `Prerelease` from string.
   * Any string can be decoded as last-resort `Unknown`
   */
  def decodePrerelease(s: String): Either[String, Prerelease] = s match {
    case milestonePattern(IntString(m)) => Right(Milestone(m))
    case rcPattern(IntString(rc)) => Right(ReleaseCandidate(rc))
    case _ => Right(Unknown(s))
  }

  /**
   * Decode semantic version from string.
   * First part must match X.Y.Z, last can be parsed either as final release or prerelease
   */
  def decodeSemver(s: String): Either[String, Semver] = s match {
    case semverPattern(IntString(major), IntString(minor), IntString(patch), "") =>
      Right(Semver(major, minor, patch, None))
    case semverPattern(IntString(major), IntString(minor), IntString(patch), preprelease) =>
      Right(Semver(major, minor, patch, decodePrerelease(preprelease).toOption))
    case _ =>
      Left(s"Version [$s] doesn't match Semantic Versioning pattern")
  }

  /**
   * Circe decoder for semantic version
   */
  implicit val semverDecoder =
    Decoder.decodeString.emap(decodeSemver)

  private implicit val prereleaseShow = new Show[Option[Prerelease]] {
    def show(prerelease: Option[Prerelease]) = prerelease match {
      case Some(p) => p.full
      case None => ""
    }
  }

  implicit val semverShow = new Show[Semver] {
    def show(version: Semver) =
      s"${version.major}.${version.minor}.${version.patch}${version.prerelease.show}"
  }
}

