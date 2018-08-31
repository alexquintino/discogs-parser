package main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object SparkUtil {
  def context(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    new SparkContext("local[8]", "testing")
  }
}
