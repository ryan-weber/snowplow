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

import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.ListBuffer

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result, GetObjectMetadataRequest}

import RefinedTypes._
import Config.SnowplowAws

object S3 {

  def getClient(awsConfig: SnowplowAws): AmazonS3 = {
    val awsCredentials = new BasicAWSCredentials(awsConfig.accessKeyId, awsConfig.secretAccessKey)

    AmazonS3ClientBuilder
      .standard()
      .withRegion(awsConfig.s3.region)
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .build()
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

  def fileExists(snowplowAws: SnowplowAws, path: S3Bucket, file: String): Boolean = {
    val s3 = getClient(snowplowAws)
    val (bucket, prefix) = splitS3Path(path)
    val request = new GetObjectMetadataRequest(bucket, prefix)
    try {
      s3.getObjectMetadata(request)
      true
    } catch {
      case _: AmazonServiceException => false
    }
  }

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
    val req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix)

    do {
      result = s3.listObjectsV2(req)
      val objects = result.getObjectSummaries.map(_.getKey)
      buffer.appendAll(objects)
    } while (result.isTruncated)
    buffer.toList
  }

  def findAtomicEventsKey(aws: SnowplowAws): Option[AtomicEventsKey] = {
    val s3 = S3.getClient(aws)
    val (bucket, prefix) = splitS3Path(aws.s3.buckets.shredded.good)

    var result: ListObjectsV2Result = null
    val req  = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix).withMaxKeys(10)
    var end: Option[AtomicEventsKey] = None

    do {
      result = s3.listObjectsV2(req)
      val objects = result.getObjectSummaries.map(_.getKey)
      end = objects.map(AtomicEventsKey.parse).collectFirst {
        case Some(key) => key
      }
    } while (result.isTruncated && end.nonEmpty)
    end.map { key =>
      // TODO: wtf?
      val q = AtomicEventsKey.unsafeCoerce("s3://" + bucket + "/" + prefix + key + "/")
      println(bucket)
      println(q)
      q
    }
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
