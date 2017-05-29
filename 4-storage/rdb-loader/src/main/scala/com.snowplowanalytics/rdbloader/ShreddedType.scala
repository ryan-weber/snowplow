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

import com.snowplowanalytics.iglu.core.SchemaKey

import RefinedTypes.S3Bucket
import Config.SnowplowAws

/**
 * Container for S3 folder with shredded JSONs ready to load
 *
 * @param prefix
 * @param vendor
 * @param name
 * @param model
 */
case class ShreddedType(prefix: S3Bucket, vendor: String, name: String, model: Int) {
  def getObjectPath: String =
    s"$prefix$vendor/$name/jsonschema/$model-"
}

object ShreddedType {

  /**
   * vendor + name + format + version + filename
   */
  private val MinShreddedPathLength = 5

  /**
   * Searches S3 for all the files we can find containing shredded paths
   *
   * @return
   */
  def discoverShreddedTypes(aws: SnowplowAws): Set[Either[String, ShreddedType]] = {
    val s3 = S3.getClient(aws)
    val (bucket, prefix) = splitS3Path(aws.s3.buckets.shredded.good)
    val types = S3.listS3(s3, bucket, prefix)
    transformPaths(types, bucket)
  }

  /**
   * IO-free function to filter, transform and group shredded types fetched with `listS3`
   *
   * @param paths list of all found S3 keys
   * @param bucket
   * @return
   */
  def transformPaths(paths: List[String], bucket: String): Set[Either[String, ShreddedType]] = {
    val transform: String => Either[String, ShreddedType] =
      transformPath(bucket, _)
    paths.filterNot(inAtomicEvents).filterNot(_.contains("$")).map(transform).toSet
  }

  private val cache = collection.mutable.HashMap.empty[String, String]

  val SnowplowHostedAssetsRoot = "s3://snowplow-hosted-assets"

  val JsonpathsPath = "/4-storage/redshift-storage/jsonpaths/"


  def discoverJsonPath(snowplowAws: SnowplowAws, shreddedType: ShreddedType): Either[String, String] = {
    val filename = s"""${toSnakeCase(shreddedType.name)}_${shreddedType.model}.json"""
    val key = s"${shreddedType.vendor}/$filename"

    cache.get(key) match {
      case Some(jsonPath) =>
        Right(jsonPath)
      case None =>
        snowplowAws.s3.buckets.jsonpathAssets match {
          case Some(assets) =>
            val path = S3Bucket.append(assets, shreddedType.vendor)
            if (S3.fileExists(snowplowAws, path, filename))
              Right(path + filename)
            else
              getSnowplowJsonPath(snowplowAws, shreddedType.vendor, filename)
          case None =>
            getSnowplowJsonPath(snowplowAws, shreddedType.vendor, filename)
        }
    }
  }

  def getSnowplowJsonPath(snowplowAws: SnowplowAws, vendor: String, filename: String): Either[String, String] = {
    val hostedAssetsBucket = getHostedAssetsBucket(snowplowAws.s3.region)
    val path = S3Bucket.append(hostedAssetsBucket, s"$JsonpathsPath$vendor")
    if (S3.fileExists(snowplowAws, path, filename)) {
      Right(path + filename)
    } else {
      Left(s"JSONPath file [$filename] not found at path [$path]")
    }
  }

  def getJsonPath(bucket: String, path: String): Either[String, String] =
    ???

  def getFile(bucket: S3Bucket, filename: String): String = {

    ???
  }

  def getHostedAssetsBucket(region: String): S3Bucket = {
    val suffix = if (region == "eu-west-1") "" else s"-$region"
    S3Bucket.unsafeCoerce(s"$SnowplowHostedAssetsRoot$suffix")
  }

  def fileExists(snowplowAws: SnowplowAws, path: String): Boolean = {

    val s3 = S3.getClient(snowplowAws)
    ???

  }

  def getTable(shreddedType: ShreddedType, databaseSchema: String): String = ???

  /**
   * Transforms CamelCase string into snake_case
   * Also replaces all hyphens with underscores
   *
   * @see https://github.com/snowplow/iglu/blob/master/0-common/schema-ddl/src/main/scala/com.snowplowanalytics/iglu.schemaddl/StringUtils.scala
   *
   * @param str string to transform
   * @return the underscored string
   */
  def toSnakeCase(str: String): String =
    str.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("-", "_")
      .replaceAll("""\.""", "_")
      .toLowerCase

  /**
   * Parse S3 key path into shredded type
   *
   * @param bucket
   * @param filePath
   * @return
   */
  // TODO: make sure it accept right filpath
  def transformPath(bucket: String, filePath: String): Either[String, ShreddedType] = {
    println(filePath)
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
            Left(s"Invalid Iglu path [$igluPath]")
        }
    }
  }

  /**
   * Split S3 path into bucket name and filePath
   *
   * @param path S3 full path without `s3://` filePath and with trailing slash
   * @return pair of bucket name and remaining path
   */
  private[rdbloader] def splitS3Path(path: S3Bucket): (String, String) = {
    path.stripPrefix("s3://").split("/").toList match {
      case head :: Nil => (head, "/")
      case head :: tail => (head, tail.mkString("/") + "/")
      case _ => throw new IllegalArgumentException("Empty bucket path was passed")  // Impossible
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

}