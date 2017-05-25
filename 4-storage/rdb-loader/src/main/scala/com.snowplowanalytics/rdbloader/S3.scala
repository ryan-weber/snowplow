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

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

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

}
