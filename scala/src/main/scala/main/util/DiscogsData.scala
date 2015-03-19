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
      case (fields, index) =>
        val releases = Array(fields(0).toLong)
        val artists = fields(1).split(",").map(_.toLong)
        val remixers = if(fields.length == 3) Array[Long]() else fields(3).split(",").map(_.toLong)
        new Track(index.toString, releases, artists, fields(2), remixers)
    }
  }

  def dedupTracks(sc: SparkContext): RDD[Track] = {
    sc.textFile(Files.DiscogsTracksDeduplicated.toString)
      .map(_.split("\t"))
      .map {
      case fields =>
        val releases = fields(1).split(",").map(_.toLong)
        val artists = fields(2).split(",").map(_.toLong)
        val remixers = if(fields.length == 4) Array[Long]() else fields(4).split(",").map(_.toLong)
        new Track(fields(0), releases, artists, fields(3), remixers)
    }
  }
}
