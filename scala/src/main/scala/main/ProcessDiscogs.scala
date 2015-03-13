package main

import main.FileManager.Files
import models.{Artist, Release, Track}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProcessDiscogs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser")
    val sc = new SparkContext(conf)
    FileManager.cleanup

    var artists = getArtists(sc)
    artists = Filters.favoriteArtists(artists, getFavoriteArtistsNames(sc, args(0)))
    val artistsWithIndex = NodeWriter.writeNodes(artists, "artist")

    var tracks = getTracks(sc)
    tracks = TrackDeduplicator.deduplicate(tracks)
    val filteredTracks = Filters.filterTracksBasedOnArtists(tracks, artists)

    var releases = getReleases(sc)
    releases = Filters.filterReleasesBasedOnTracks(releases, filteredTracks)
    releases = Filters.filterReleasesBasedOnMasters(releases)
    val releasesWithIndex = NodeWriter.writeNodes(releases, "tracklist")

    Relationships.writeArtistToReleases(artistsWithIndex, releasesWithIndex)

    val finalTrackList = Filters.filterTracksBasedOnReleases(tracks, releases)
    val tracksWithIndex = NodeWriter.writeNodes(finalTrackList, "track")

    Relationships.writeReleasesToTracks(releasesWithIndex, tracksWithIndex)
    Relationships.writeArtistsToTracks(artistsWithIndex, tracksWithIndex)
    Relationships.writeRemixersToTracks(artistsWithIndex, tracksWithIndex)

    MergeOutput.mergeAll
  }

  def getArtists(sc: SparkContext): RDD[Artist] = {
    sc.textFile(Files.DiscogsArtists.toString).map(_.split("\t")).map { case fields:Array[String] => new Artist(fields(0), fields(1)) }
  }

  def getFavoriteArtistsNames(sc: SparkContext, file: String): RDD[String] = {
    sc.textFile(file)
  }

  def getTracks(sc: SparkContext): RDD[Track] = {
    sc.textFile(Files.DiscogsTracks.toString).map(_.split("\t")).filter(_.size > 2).zipWithIndex().map {
      case (fields, index) => if (fields.length == 3)
        new Track(index.toString, fields(0), fields(1), fields(2), "")
      else
        new Track(index.toString, fields(0), fields(1), fields(2), fields(3))
    }
  }

  def getReleases(sc:SparkContext): RDD[Release] = {
    sc.textFile(Files.DiscogsReleases.toString).map(_.split("\t")).filter(_.size == 4).map(fields => new Release(fields(0), fields(1), fields(2), fields(3)))
  }
}
