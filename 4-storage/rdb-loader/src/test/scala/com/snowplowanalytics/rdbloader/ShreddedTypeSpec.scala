/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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

// specs2
import com.snowplowanalytics.rdbloader.RefinedTypes.S3Bucket
import org.specs2.Specification


class ShreddedTypeSpec extends Specification { def is = s2"""
  Targets parse specification

    Transform correct S3 path $e1
    Fail to transform path without valid vendor $e2
    Fail to transform path without file $e3
    Transform correct S3 path without prefix $e4
    Transform batch of paths $e5
    Omit atomic-events path $e6

  """

  def e1 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001"
    val expectedPrefix = S3Bucket.unsafeCoerce("s3://rdb-test/cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42")
    val expected = ShreddedType(expectedPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1, "nonatomic")
    val result = ShreddedType.transformPath("nonatomic", "rdb-test", path)
    result must beRight(expected)
  }

  def e2 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/submit_form/jsonschema/1-0-0/part-00000-00001"
    val result = ShreddedType.transformPath("nonatomic", "rdb-test", path)
    result must beLeft
  }

  def e3 = {
    val path = "cross-batch-test/shredded-archive/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0"
    val result = ShreddedType.transformPath("nonatomic", "rdb-test", path)
    result must beLeft
  }

  def e4 = {
    val path = "com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001"
    val result = ShreddedType.transformPath("nonatomic", "rdb-test", path)
    val expected = ShreddedType(S3Bucket.unsafeCoerce("s3://rdb-test"), "com.snowplowanalytics.snowplow", "submit_form", 1, "nonatomic")
    result must beRight(expected)
  }

  def e5 = {
    val paths = List(
      "shredded/run%3D2017-04-27-14-39-42/com.acme/context/jsonschema/2-1-0/part-00000-00001",
      "shredded/run%3D2017-04-27-14-39-42/com.acme/context/jsonschema/2-2-0/random-file",
      "shredded/run%3D2017-04-27-14-39-42/com.acme/context/jsonschema/1-1-0/part-00000-00001",
      "shredded/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/submit_form/jsonschema/1-0-0/part-00000-00001",
      "shredded/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-0/part-00000-00001",
      "shredded/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-1-0/part-00000-00002",
      "shredded/run%3D2017-04-27-14-39-42/com.snowplowanalytics.snowplow/geolocation_context/jsonschema/1-0-3/part-00000-00003"
    )

    val commonPrefix = S3Bucket.unsafeCoerce("s3://snowplow-events/shredded/run%3D2017-04-27-14-39-42")
    val expected = Set(
      ShreddedType(commonPrefix, "com.acme", "context", 1, "atomic"),
      ShreddedType(commonPrefix, "com.acme", "context", 2, "atomic"),
      ShreddedType(commonPrefix, "com.snowplowanalytics.snowplow", "submit_form", 1, "atomic"),
      ShreddedType(commonPrefix, "com.snowplowanalytics.snowplow", "geolocation_context", 1, "atomic")
    ).map(Right.apply)

    val result = ShreddedType.transformPaths(paths, "atomic", "snowplow-events")

    result must beEqualTo(expected)
  }

  def e6 = {
    val paths = List(
      "shredded/run%3D2017-04-27-14-39-42/atomic-events/part-00001",
      "shredded/run%3D2017-04-27-14-39-42/atomic-events/part-00002",
      "shredded/run%3D2017-04-27-14-39-42/atomic-events/part-00003",
      "shredded/run%3D2017-04-27-14-39-42/com.acme/context/jsonschema/1-1-0/part-00000-00001"
    )

    val commonPrefix = S3Bucket.unsafeCoerce("s3://snowplow-events/shredded/run%3D2017-04-27-14-39-42")
    val expected = Set(ShreddedType(commonPrefix, "com.acme", "context", 1, "atomic")).map(Right.apply)

    val result = ShreddedType.transformPaths(paths, "atomic", "snowplow-events")
    result must beEqualTo(expected)
  }
}
