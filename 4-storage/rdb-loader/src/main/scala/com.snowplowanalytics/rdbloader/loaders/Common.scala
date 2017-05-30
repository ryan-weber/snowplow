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
package com.snowplowanalytics.rdbloader.loaders

import com.snowplowanalytics.rdbloader.RefinedTypes.AtomicEventsKey

object Common {

  val EventsTable = "events"

  val ManifestTable = "manifest"

  def getEventsTable(databaseSchema: String): String =
    databaseSchema + "." + EventsTable

  def getManifestTable(databaseSchema: String): String =
    databaseSchema + "." + ManifestTable

  //                            year     month      day        hour       minute     second
  val atomicSubpathPattern = "(run=[0-9]{4}-[0-1][0-9]-[0-3][0-9]-[0-2][0-9]-[0-6][0-9]-[0-6][0-9]/atomic-events)".r

  /**
   * Check if key has valid format for atomic-events folder
   *
   * @param s3key S3 path
   * @return true if key contains `run=YYYY-MM-dd-HH-mm-ss/atomic-events` part
   */
  def isAtomicEvent(s3key: String): Boolean =
    AtomicEventsKey.extractAtomicSubpath(s3key).isDefined

  def extractAtomicEventsSubpath(s3key: AtomicEventsKey): String = {
    // .get is safe because AtomicEventsKey proven to have this part
    atomicSubpathPattern.findFirstIn(s3key).get
  }

}
