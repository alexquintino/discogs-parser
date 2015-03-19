package main.deduplication

import models.Track
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object TrackDeduplicator {
  def deduplicate(tracks: RDD[Track]): RDD[Track] = {
    val tracksWithHash = tracks.map(track => (track.hash, track))
    tracksWithHash.reduceByKey(mergeTracks(_, _)).map { case (hash, track) => track}
  }

  def mergeTracks(track1:Track, track2:Track): Track = {
    track1.addReleases(track2.releases)
  }
}
