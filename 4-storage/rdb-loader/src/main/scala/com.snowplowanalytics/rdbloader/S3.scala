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

// Scala
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.ListBuffer

// AWS Java SDK
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, GetObjectMetadataRequest}

// This project
import RefinedTypes._
import Config.SnowplowAws

trait S3 {
  def fileExists(path: S3Bucket, file: String): Boolean
  def findAtomicEventsKey(): Option[S3Bucket]
  def listS3(s3folder: S3Bucket): List[S3Key]
}

object S3 {

  class AwsS3Client(client: AmazonS3) extends S3 {
    /**
     * Check if some `file` exists in S3 `path`
     *
     * @param path valid S3 path (with trailing slash)
     * @param file file name
     * @return true if file exists, false if file doesn't exist or not available
     */
    def fileExists(path: S3Bucket, file: String): Boolean = {
      val (bucket, prefix) = splitS3Path(path)
      val request = new GetObjectMetadataRequest(bucket, prefix)
      try {
        client.getObjectMetadata(request)
        true
      } catch {
        case _: AmazonServiceException => false
      }
    }

    def findAtomicEventsKey(aws: SnowplowAws): Option[S3Bucket] = {
      val (bucket, prefix) = splitS3Path(aws.s3.buckets.shredded.good)

      var result: ListObjectsV2Result = null
      val req  = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix).withMaxKeys(10)
      var end: Option[String] = None

      do {
        result = client.listObjectsV2(req)
        val objects = result.getObjectSummaries.map(_.getKey)
        end = objects.map(AtomicEventsKey.extractAtomicSubpath).collectFirst {
          case Some(subpath) => subpath   // run=YYYY-MM-dd-HH-mm-ss/atomic-events
        }
      } while (result.isTruncated && end.nonEmpty)

      end.map { subpath => S3Bucket.unsafeCoerce("s3://" + bucket + "/" + prefix + subpath + "/") }
    }


  }

  def getClient(awsConfig: SnowplowAws): S3 = {
    val awsCredentials = new BasicAWSCredentials(awsConfig.accessKeyId, awsConfig.secretAccessKey)

    val client = AmazonS3ClientBuilder
      .standard()
      .withRegion(awsConfig.s3.region)
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .build()

    new AwsS3Client(client)
  }


  /**
   * Split S3 path into bucket name and filePath
   *
   * @param path S3 full path without `s3://` filePath and with trailing slash
   * @return pair of bucket name and remaining path ("some-bucket", "some/prefix/")
   */
  private[rdbloader] def splitS3Path(path: S3Bucket): (String, String) = splitS3Path(path)

  private[rdbloader] def splitS3Key(key: S3Key): (String, String) = splitS3(key)

  private def splitS3(path: String): (String, String) = {
    path.stripPrefix("s3://").split("/").toList match {
      case head :: Nil => (head, "/")
      case head :: tail => (head, tail.mkString("/") + "/")
      case _ => throw new IllegalArgumentException("Empty bucket path was passed")  // Impossible
    }
  }

  /**
   * Check if some `file` exists in S3 `path`
   *
   * @param snowplowAws Snowplow AWS configuration
   * @param path valid S3 path (with trailing slash)
   * @param file file name
   * @return true if file exists, false if file doesn't exist or not available
   */
  def fileExists(snowplowAws: SnowplowAws, path: S3Bucket, file: String): Boolean = {
    val s3 = getClient(snowplowAws)
    s3.fileExists(path, file)
  }

  /**
   * Find first occurrence of key matching atomic events pattern
   * in `shredded.good` bucket. Strips filename part
   *
   * @param aws Snowplow AWS configuration
   * @return first
   */
  def findAtomicEventsKey(aws: SnowplowAws): Option[S3Bucket] = {
    val s3 = S3.getClient(aws)
    s3.findAtomicEventsKey()
  }

  def listS3(s3: AmazonS3, s3folder: S3Bucket): List[S3Key] = {
    val (bucket, prefix) = splitS3Path(s3folder)

    var result: ListObjectsV2Result = null
    val buffer = ListBuffer.empty[String]
    val req  = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix)

    do {
      result = s3.listObjectsV2(req)
      val objects = result.getObjectSummaries.map(_.getKey)
      buffer.appendAll(objects)
    } while (result.isTruncated)
    buffer.map(key => S3Key.unsafeCoerce(s"s3://$bucket/$key")).toList
  }
}
