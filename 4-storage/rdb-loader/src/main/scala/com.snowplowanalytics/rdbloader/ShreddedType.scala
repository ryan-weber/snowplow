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

// Scala
import scala.collection.SortedSet
import scala.collection.breakOut

// Iglu core
import com.snowplowanalytics.iglu.core.SchemaKey

// This project
import Utils.toSnakeCase
import RefinedTypes.S3Bucket
import Config.SnowplowAws

/**
 * Container for S3 folder with shredded JSONs ready to load
 * Usually it represents self-describing event or custom/derived context
 *
 * @param prefix full S3 path, where folder with shredded JSONs resides
 * @param vendor self-describing type's vendor
 * @param name self-describing type's name
 * @param model self-describing type's SchemaVer model
 */
case class ShreddedType(prefix: S3Bucket, vendor: String, name: String, model: Int) {
  /**
   * Get S3 prefix which Redshift should LOAD FROM
   */
  def getObjectPath: String =
    s"$prefix$vendor/$name/jsonschema/$model-"
}

/**
 * Companion object for `ShreddedType` containing discovering functions
 */
object ShreddedType {

  /**
   * Basis for Snowplow hosted assets bucket.
   * Can be modified to match specific region
   */
  val SnowplowHostedAssetsRoot = "s3://snowplow-hosted-assets"

  /**
   * Default JSONPaths path
   */
  val JsonpathsPath = "/4-storage/redshift-storage/jsonpaths/"

  /**
   * vendor + name + format + version + filename
   */
  private val MinShreddedPathLength = 5

  private val cache = collection.mutable.HashMap.empty[String, String]

  /**
   * Searches S3 for all the files we can find containing shredded paths
   *
   * @param aws Snowplow AWS configuration
   * @return sorted set (only unique values) of discovered shredded type results,
   *         where result can be either shredded type, or discovery error
   */
  def discoverShreddedTypes(aws: SnowplowAws): SortedSet[Either[DiscoveryError, ShreddedType]] = {
    val s3 = S3.getClient(aws)
    val (bucket, prefix) = S3.splitS3Path(aws.s3.buckets.shredded.good)
    val types = S3.listS3(s3, bucket, prefix)
    transformPaths(types, bucket)
  }

  /**
   * IO-free function to filter, transform and group shredded types fetched with `listS3`
   *
   * @param paths list of all found S3 keys
   * @param bucket S3 bucket name (without `s3://` prefix)
   * @return sorted set (only unique values) of discovered shredded type results,
   *         where result can be either shredded type, or discovery error
   */
  def transformPaths(paths: List[String], bucket: String): SortedSet[Either[DiscoveryError, ShreddedType]] = {
    val transform: String => Either[DiscoveryError, ShreddedType] =
      transformPath(bucket, _)
    val list: List[Either[DiscoveryError, ShreddedType]] =
      paths.filterNot(inAtomicEvents).filterNot(specialFile).map(transform)(breakOut)
    toSortedSet(list)
  }

  /**
   * Check where JSONPaths file for particular shredded type exists:
   * in cache, in custom `s3.buckets.jsonpath_assets` S3 path or in Snowplow hosted assets bucket
   * and return full JSONPaths S3 path
   *
   * @param snowplowAws Snowplow AWS configuration
   * @param shreddedType some shredded type (self-describing event or context)
   * @return full valid s3 path (with `s3://` prefix)
   */
  def discoverJsonPath(snowplowAws: SnowplowAws, shreddedType: ShreddedType): Either[DiscoveryError, String] = {
    val filename = s"""${toSnakeCase(shreddedType.name)}_${shreddedType.model}.json"""
    val key = s"${shreddedType.vendor}/$filename"

    cache.get(key) match {
      case Some(jsonPath) => Right(jsonPath)
      case None =>
        val result = snowplowAws.s3.buckets.jsonpathAssets match {
          case Some(assets) =>
            val path = S3Bucket.append(assets, shreddedType.vendor)
            if (S3.fileExists(snowplowAws, path, filename)) Right(path + filename)
            else getSnowplowJsonPath(snowplowAws, shreddedType.vendor, filename)
          case None => getSnowplowJsonPath(snowplowAws, shreddedType.vendor, filename)
        }

        // Cache successful results
        result match {
          case Right(path) => cache.put(key, path)
          case _ => ()
        }

        result
    }
  }

  /**
   * Build valid table name for some shredded type
   *
   * @param shreddedType shredded type for self-describing event or context
   * @param databaseSchema database schema
   * @return valid table name
   */
  def getTable(shreddedType: ShreddedType, databaseSchema: String): String =
    s"${toSnakeCase(shreddedType.vendor)}_${toSnakeCase(shreddedType.name)}_${shreddedType.model}"

  /**
   * Check that JSONPaths file exists in Snowplow hosted assets bucket
   *
   * @param snowplowAws Snowplow AWS configuration
   * @param vendor self-describing's type vendor
   * @param filename JSONPaths filename (without prefixes)
   * @return full S3 key if file exists, discovery error otherwise
   */
  def getSnowplowJsonPath(snowplowAws: SnowplowAws, vendor: String, filename: String): Either[DiscoveryError, String] = {
    val hostedAssetsBucket = getHostedAssetsBucket(snowplowAws.s3.region)
    val path = S3Bucket.append(hostedAssetsBucket, s"$JsonpathsPath$vendor")
    if (S3.fileExists(snowplowAws, path, filename)) {
      Right(path + filename)
    } else {
      Left(DiscoveryError(s"JSONPath file [$filename] not found at path [$path]"))
    }
  }

  /**
   * Get Snowplow hosted assets S3 bucket for specific region
   *
   * @param region valid AWS region
   * @return AWS S3 path such as `s3://snowplow-hosted-assets-us-west-2/`
   */
  def getHostedAssetsBucket(region: String): S3Bucket = {
    val suffix = if (region == "eu-west-1") "" else s"-$region"
    S3Bucket.unsafeCoerce(s"$SnowplowHostedAssetsRoot$suffix")
  }

  /**
   * Parse S3 key path into shredded type
   *
   * @param bucket
   * @param filePath
   * @return
   */
  // TODO: make sure it accept right filepath
  def transformPath(bucket: String, filePath: String): Either[DiscoveryError, ShreddedType] = {
    filePath.split("/").reverse.splitAt(MinShreddedPathLength) match {
      case (reverseSchema, reversePath) =>
        val igluPath = reverseSchema.tail.reverse.mkString("/")
        SchemaKey.fromPath(igluPath) match {
          case Some(key) =>
            val rootPath = reversePath.reverse.mkString("/")
            val prefix = S3Bucket.unsafeCoerce("s3://" + bucket + "/" + rootPath)
            val result = ShreddedType(prefix, key.vendor, key.name, key.version.model)
            Right(result)
          case None =>
            Left(DiscoveryError(s"Shredded type discovered in invalid Iglu path [$igluPath]"))
        }
    }
  }

  /**
   * Predicate to check if S3 key is in atomic-events folder
   *
   * @param key full S3 path
   * @return true if path contains `atomic-events`
   */
  def inAtomicEvents(key: String): Boolean =
    key.split("/").contains("atomic-events")

  /**
   * Predicate to check if S3 key is special file like `$folder$`
   *
   * @param key full S3 path
   * @return true if path contains `atomic-events`
   */
  def specialFile(key: String): Boolean =
    key.contains("$")

  /**
   * Ordering instance to help build `SortedSet` of transformation results from `List`
   */
  private implicit object EitherOrdering extends Ordering[Either[DiscoveryError, ShreddedType]] {
    def compare(x: Either[DiscoveryError, ShreddedType], y: Either[DiscoveryError, ShreddedType]): Int = {
      x match {
        case Left(xe) => y match {
          case Right(_) => -1
          case Left(ye) => implicitly[Ordering[String]].compare(xe.message, ye.message)
        }
        case Right(xr) => y match {
          case Left(_) => 1
          case Right(yr) =>
            val ordering = implicitly[Ordering[(String, String, String, Int)]]
            ordering.compare(
              (xr.prefix, xr.vendor, xr.name, xr.model),
              (yr.prefix, yr.vendor, yr.name, yr.model)
            )
        }
      }
    }
  }

  /**
   * Helper function with `Ordering` instance in scope
   */
  private def toSortedSet(list: List[Either[DiscoveryError, ShreddedType]]): SortedSet[Either[DiscoveryError, ShreddedType]] =
    list.to[SortedSet]
}