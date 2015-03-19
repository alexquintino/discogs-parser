package main

import main.util._
import FileManager.Files
import main.deduplication.TrackDeduplicator
import models.{Artist, Release, Track}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object ProcessDiscogs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser")
    val sc = new SparkContext(conf)

    var artists = DiscogsData.artists(sc)
    artists = Filters.favoriteArtists(artists, getFavoriteArtistsNames(sc, args(0))).cache()
    val artistsWithIndex = NodeWriter.writeNodes(artists, "artist").cache()

    val tracks = DiscogsData.dedupTracks(sc).cache()
    val filteredTracks = Filters.filterTracksBasedOnArtists(tracks, artists)

    var releases = DiscogsData.releases(sc)
    releases = Filters.filterReleasesBasedOnTracks(releases, filteredTracks)
    releases = Filters.filterReleasesBasedOnMasters(releases)
    val releasesWithIndex = NodeWriter.writeNodes(releases, "tracklist")

    Relationships.writeArtistToReleases(artistsWithIndex, releasesWithIndex)

    val finalTrackList = Filters.filterTracksBasedOnReleases(tracks, releases)
    val tracksWithIndex = NodeWriter.writeNodes(finalTrackList, "track")

    Relationships.writeReleasesToTracks(releasesWithIndex, tracksWithIndex)
    Relationships.writeArtistsToTracks(artistsWithIndex, tracksWithIndex)
    Relationships.writeRemixersToTracks(artistsWithIndex, tracksWithIndex)
  }

  def getFavoriteArtistsNames(sc: SparkContext, file: String): RDD[String] = {
    sc.makeRDD(Source.fromFile(file).getLines().toStream)
  }
}
