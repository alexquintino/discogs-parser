package main

import main.util.{FileManager, DiscogsData}
import FileManager.Files
import main.deduplication.TrackDeduplicator
import main.util.DiscogsData
import models.Track
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

object DeduplicateTracks {
  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser")
    val sc = new SparkContext(conf)

    val deduplicatedTracks = TrackDeduplicator.deduplicate(DiscogsData.tracks(sc))
    deduplicatedTracks.saveAsTextFile(Files.DiscogsTracksDeduplicated.toString)
  }


}
