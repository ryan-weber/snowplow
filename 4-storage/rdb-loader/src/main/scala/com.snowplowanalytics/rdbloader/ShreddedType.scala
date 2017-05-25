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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}

import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.ListBuffer
import com.snowplowanalytics.iglu.core.SchemaKey
import RefinedTypes.S3Bucket
import com.snowplowanalytics.rdbloader.Config.SnowplowAws

/**
 * Container for S3 folder with shredded JSONs ready to load
 *
 * @param prefix
 * @param vendor
 * @param name
 * @param model
 * @param databaseSchema
 */
case class ShreddedType(prefix: S3Bucket, vendor: String, name: String, model: Int, databaseSchema: String) {
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
   * @param databaseSchema
   * @return
   */
  def discoverShreddedTypes(aws: SnowplowAws, databaseSchema: String): Set[Either[String, ShreddedType]] = {
    val s3 = S3.getClient(aws)
    val (bucket, prefix) = splitS3Path(aws.s3.buckets.shredded.good)
    val types = listS3(s3, bucket, prefix)
    transformPaths(types, databaseSchema, bucket)
  }

  /**
   * IO-free function to filter, transform and group shredded types fetched with `listS3`
   *
   * @param paths list of all found S3 keys
   * @param databaseSchema
   * @param bucket
   * @return
   */
  def transformPaths(paths: List[String], databaseSchema: String, bucket: String): Set[Either[String, ShreddedType]] = {
    val transform: String => Either[String, ShreddedType] =
      transformPath(databaseSchema, bucket, _)
    paths.filterNot(inAtomicEvents).map(transform).toSet
  }

  def discoverJsonPath(snowplowAws: SnowplowAws, shreddedType: ShreddedType): Either[String, String] = {
    ???
  }

  /**
   * Parse S3 key path into shredded type
   *
   * @param bucket
   * @param filePath
   * @return
   */
  def transformPath(databaseSchema: String, bucket: String, filePath: String): Either[String, ShreddedType] = {
    filePath.split("/").reverse.splitAt(MinShreddedPathLength) match {
      case (reverseSchema, reversePath) =>
        val igluPath = reverseSchema.tail.reverse.mkString("/")
        SchemaKey.fromPath(igluPath) match {
          case Some(key) =>
            val rootPath = reversePath.reverse.mkString("/")
            val prefix = S3Bucket.unsafeCoerce("s3://" + bucket + "/" + rootPath)
            val result = ShreddedType(prefix, key.vendor, key.name, key.version.model, databaseSchema)
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

  /**
    * Helper method to get **all** keys from bucket.
    * Unlike usual `s3.listObjects` it won't truncate response after 1000 items
    * TODO: check if we really can reach 1000 items - we surely can
    *
    * @param s3
    * @param bucket
    * @param prefix
    * @return
    */
  def listS3(s3: AmazonS3, bucket: String, prefix: String): List[String] = {
    var result: ListObjectsV2Result = null
    val buffer = ListBuffer.empty[String]
    val req  = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix)

    do {
      result = s3.listObjectsV2(req)
      val objects = result.getObjectSummaries.map(_.getKey)
      buffer.appendAll(objects)
    } while (result.isTruncated)
    buffer.toList
  }

  def findFirst(aws: SnowplowAws, predicate: String => Boolean): Option[String] = {
    val s3 = S3.getClient(aws)
    ???
  }

  def listS3(s3: AmazonS3, s3folder: S3Bucket): List[String] = {
    val (bucket, prefix) = splitS3Path(s3folder)

    var result: ListObjectsV2Result = null
    val buffer = ListBuffer.empty[String]
    val req  = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix)

    do {
      result = s3.listObjectsV2(req)
      val objects = result.getObjectSummaries.map(_.getKey)
      buffer.appendAll(objects)
    } while (result.isTruncated)
    buffer.toList
  }

}