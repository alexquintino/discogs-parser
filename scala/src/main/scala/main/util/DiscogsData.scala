package main.util

import FileManager.Files
import models.{Artist, Release, Track}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object DiscogsData {

  def artists(sc: SparkContext): RDD[Artist] = {
    sc.textFile(Files.DiscogsArtists.toString)
      .map(_.split("\t"))
      .map { case fields:Array[String] => new Artist(fields(0), fields(1)) }
  }

  def releases(sc:SparkContext): RDD[Release] = {
    sc.textFile(Files.DiscogsReleases.toString)
      .map(_.split("\t"))
      .filter(_.size == 4)
      .map(fields => new Release(fields(0), fields(1), fields(2), fields(3)))
  }

  def tracks(sc: SparkContext): RDD[Track] = {
    sc.textFile(Files.DiscogsTracks.toString)
      .map(_.split("\t"))
      .filter(_.size > 2)
      .zipWithIndex()
      .map {
      case (fields, index) => if (fields.length == 3)
        new Track(index.toString, fields(0), fields(1), fields(2), "")
      else
        new Track(index.toString, fields(0), fields(1), fields(2), fields(3))
    }
  }

  def dedupTracks(sc: SparkContext): RDD[Track] = {
    sc.textFile(Files.DiscogsTracksDeduplicated.toString)
      .map(_.split("\t"))
      .map {
      case fields => if (fields.length == 4)
        new Track(fields(0), fields(1), fields(2), fields(3), "")
      else
        new Track(fields(0), fields(1), fields(2), fields(3), fields(4))
    }
  }
}
