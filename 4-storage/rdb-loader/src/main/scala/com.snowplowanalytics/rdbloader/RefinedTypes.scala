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

import loaders.Common

import shapeless.tag, tag._

import cats.syntax.either._

import io.circe.Decoder

/**
  * Module with auxiliary paths to add type-safety into configuration parsing
  */
object RefinedTypes {

  sealed trait S3BucketTag

  /**
    * Refined type for AWS S3 bucket, allowing only valid S3 paths
    */
  // TODO: rename to S3Folder
  type S3Bucket = String @@ S3BucketTag

  object S3Bucket extends tag.Tagger[S3BucketTag] {
    def appendTrailingSlash(s: String): String =
      if (s.endsWith("/")) s
      else s + "/"

    // TODO: s3n, s3a. But in the end it should be always s3
    def parse(s: String): Either[String, S3Bucket] = s match {
      case _ if !s.startsWith("s3://") => "Bucket name must start with s3://".asLeft
      case _ if s.length > 1024        => "Key length cannot be more than 1024 symbols".asLeft
      case _                           => apply(appendTrailingSlash(s)).asRight
    }

    def unsafeCoerce(s: String): S3Bucket =
      apply(appendTrailingSlash(s).asInstanceOf[S3Bucket])

    def append(s3Bucket: S3Bucket, s: String): S3Bucket = {
      val normalized = if (s.endsWith("/")) s else s + "/"
      unsafeCoerce(s3Bucket + normalized)
    }

  }

  implicit val bucketDecoder =
    Decoder.decodeString.emap(S3Bucket.parse)


  // TODO: probably can be CopyStatementTag
  sealed trait SqlStringTag

  type SqlString = String @@ SqlStringTag

  object SqlString extends tag.Tagger[SqlStringTag] {
    def unsafeCoerce(s: String) = apply(s)
  }

  sealed trait AtomicEventsKeyTag

  /**
   * Valid string of format
   * `s3://some-bucket/artbitrary/path/run=YYYY-MM-dd-HH-mm-ss/atomic-events/somefile`
   */
  type AtomicEventsKey = String @@ AtomicEventsKeyTag

  object AtomicEventsKey extends tag.Tagger[AtomicEventsKeyTag] {
    def stripFilePart(key: AtomicEventsKey): S3Bucket = {
      val string = key.split("atomic-events/").dropRight(1).mkString("atomic-events/")
      S3Bucket.unsafeCoerce(string)
    }

    def parse(s: String): Option[AtomicEventsKey] =
      Common.atomicPathPattern.findFirstIn(s).map(_.asInstanceOf[AtomicEventsKey])

    def unsafeCoerce(s: String) = apply(s)
  }
}
