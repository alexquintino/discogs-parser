package main

import models.{Track, Artist, Release}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import FileManager._

object Relationships {

  def writeArtistToReleases(artists: RDD[(Artist, Long)], releases: RDD[(Release, Long)]) {
    extractArtistsReleasesRelationships(artists, releases)
      .map(_.mkString("\t"))
      .saveAsTextFile(Files.ArtistReleaseRelationship.toString)
  }

  def writeReleasesToTracks(releases: RDD[(Release, Long)], tracks: RDD[(Track, Long)]) {
    extractReleasesTracksRelationships(releases, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile(Files.TracklistTrackRelationship.toString)
  }

  def writeArtistsToTracks(artists: RDD[(Artist, Long)], tracks: RDD[(Track, Long)]) {
    extractArtistsTracksRelationships(artists, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile(Files.ArtistTracksRelationship.toString)
  }

  def writeRemixersToTracks(artists: RDD[(Artist, Long)], tracks: RDD[(Track, Long)]) {
    extractRemixersTracksRelationships(artists, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile(Files.RemixerTrackRelationship.toString)
  }

  def extractArtistsReleasesRelationships(artists: RDD[(Artist, Long)], releases: RDD[(Release, Long)]): RDD[List[Any]] = {
    val releasesMap =  releases.flatMap(restructureRelease)
    artistsMap(artists).join(releasesMap)
              .map(extractArtistReleaseRelationship)
  }

  def extractReleasesTracksRelationships(releases: RDD[(Release, Long)], tracks: RDD[(Track, Long)]): RDD[List[Any]] = {
    val releasesMap = releases.map { case (rel, index) => (rel.id, index.toString) }
    val tracksMap = tracks.flatMap(restructureTrack)
    releasesMap.join(tracksMap)
                .map(extractReleaseTrackRelationship)
  }

  def extractArtistsTracksRelationships(artists: RDD[(Artist, Long)], tracks: RDD[(Track, Long)]): RDD[List[Any]] = {
    val tracksMap = tracks.flatMap(splitArtistsInTrack)
    artistsMap(artists).join(tracksMap)
              .map(extractArtistTrackRelationship)
  }

  def extractRemixersTracksRelationships(artists: RDD[(Artist, Long)], tracks: RDD[(Track, Long)]): RDD[List[Any]] = {
    val tracksMap = tracks.filter { case (track, _) => track.hasRemixers }.flatMap(splitRemixersInTrack)
    artistsMap(artists).join(tracksMap)
              .map(extractRemixerTrackRelationship)
  }

  def extractArtistReleaseRelationship(rel: (String, (String, String))): List[Any] = {
    List(rel._2._1, rel._2._2, "HAS_TRACKLIST")
  }

  def extractReleaseTrackRelationship(rel: (String, (String, String))): List[Any] = {
    List(rel._2._1, rel._2._2, "HAS_TRACK")
  }

  def extractArtistTrackRelationship(rel: (String, (String, String))): List[Any] = {
    List(rel._2._1, rel._2._2, "HAS_TRACK")
  }

  def extractRemixerTrackRelationship(rel: (String, (String, String))): List[Any] = {
    List(rel._2._1, rel._2._2, "HAS_REMIX")
  }

  def restructureRelease(release: (Release, Long)): Array[(String, String)] = {
    release._1.artists.map { artist => (artist, release._2.toString) }
  }

  def restructureTrack(track: (Track, Long)): Array[(String, String)] = {
    track._1.releases.map(releaseId => (releaseId, track._2.toString))
  }

  def artistsMap(artists: RDD[(Artist, Long)]): RDD[(String, String)] = artists.map { case (artist, index) => (artist.id, index.toString) }

  def splitArtistsInTrack(track: (Track, Long)): Array[(String, String)] = {
    track._1.artists.map { artist => (artist, track._2.toString) }
  }

  def splitRemixersInTrack(track: (Track, Long)): Array[(String, String)] = {
    track._1.remixers.map { artist => (artist, track._2.toString) }
  }
}
