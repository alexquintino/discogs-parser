package main

import models.Track
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object TrackDeduplicator {
  def deduplicate(tracks: RDD[Track]): RDD[Track] = {
    val tracksWithHash = tracks.map(track => (track.hashCode, track))
    tracksWithHash.reduceByKey(mergeTracks(_, _)).map { case (hash, track) => track}
  }

  def mergeTracks(track1:Track, track2:Track): Track = {
    track1.addReleases(track2.releases)
  }
}
