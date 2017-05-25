package com.snowplowanalytics.rdbloader

import loaders.PostgresqlLoader.PostgresLoadError
import Config._


object Monitoring {

  trait Tracker {
    def trackError(error: Throwable): Unit = ???
    def trackSuccess(message: String): Unit = ???
  }

  def initializeTracking(monitoring: Monitoring): Option[Tracker] = {
    ???
  }

  def trackLoadSucceeded(): Unit = {
    println("Successfully loaded")
  }
  def trackLoadFailed(error: PostgresLoadError): Unit = {
    println("Error during load")
  }
}
