package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object OutputNodesAndRelationships {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser Nodes and Relationships")
    val sc = new SparkContext(conf)

    val artists = getArtists(sc.textFile("output/artists_with_ids", 1))
    val artistsLastIndex = artists.map(_(0).toLong).max
    savaArtistsNodes(artists)


    // release_id / master_id / title / main_artists
    val releases = getReleases(sc.textFile("output/releases", 1), artistsLastIndex)
    val releasesLastIndex = releases.map(_(0).toLong).max
    saveReleasesNodes(releases)

    // release_id / artists / title / remixers
    val tracks = getTracks(sc.textFile("output/tracks"), releasesLastIndex)
    saveTracksNodes(tracks)


    // Relationships
    extractArtistsReleasesRelationships(artists, releases)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/artist_release_relationships")

    extractReleasesTracksRelationships(releases, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/tracklist_track_relationships")

    extractArtistsTracksRelationships(artists, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/artist_track_relationships")


  }


  def extractArtistsReleasesRelationships(artists: RDD[Array[String]], releases: RDD[Array[String]]): RDD[List[Any]] = {
    val artistsMap = artists.map(artist => (artist(1), artist(0)))
    val releasesMap =  releases.flatMap(restructureRelease)
    artistsMap.join(releasesMap)
              .map(extractArtistReleaseRelationship)
  }

  def extractReleasesTracksRelationships(releases: RDD[Array[String]], tracks: RDD[Array[String]]): RDD[List[Any]] = {
    val releasesMap = releases.map(rel => (rel(1), rel(0)))
    val tracksMap = tracks.map(track => (track(1), track(0)))
    releasesMap.join(tracksMap)
                .map(extractReleaseTrackRelationship)
  }

  def extractArtistsTracksRelationships(artists: RDD[Array[String]], tracks: RDD[Array[String]]): RDD[List[Any]] = {
    val artistsMap = artists.map(artist => (artist(1), artist(0)))
    val tracksMap = tracks.flatMap(splitArtistsInTrack)
    artistsMap.join(tracksMap)
              .map(extractArtistTrackRelationship)
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

  def restructureRelease(release: Array[String]): Array[(String, String)] = {
    val artists = release(4)
    artists.split(",").map{
      artist => (artist, release(0)) //from (id, artists) to (artistId, id)
    }
  }

  def splitArtistsInTrack(track: Array[String]): Array[(String, String)] = {
    val artists = track(2)
    artists.split(",").map {
      artist => (artist, track(0))
    }
  }



  def savaArtistsNodes(artists: RDD[Array[String]]) {
    artists.map(artist => artist ++ Array("Artist")).map(_.mkString("\t")).saveAsTextFile("output/artists_nodes")
  }

  def saveReleasesNodes(releases: RDD[Array[String]]) {
    releases.map(release => Array(release(0), release(1), release(3), "Tracklist").mkString("\t")).saveAsTextFile("output/tracklists_nodes")
  }

  def saveTracksNodes(tracks: RDD[Array[String]]) {
    tracks.map(track => Array(track(0), track(3), "Track").mkString("\t")).saveAsTextFile("output/tracks_nodes")
  }

  def getArtists(artists: RDD[String]): RDD[Array[String]] = {
    artists.map(_.split("\t"))
            .map(artist => Array(artist(0), artist(0), artist(1)))
  }
  def getReleases(releases: RDD[String], artistsLastIndex: Long): RDD[Array[String]] = {
    releases.map(_.split("\t"))
            .map(release => Array((release(0).toLong + artistsLastIndex).toString, release(0), release(1), release(2), release(3)))
  }
  def getTracks(tracks: RDD[String], releasesLastIndex: Long ): RDD[Array[String]] = {
    tracks.map(_.split("\t"))
          .zipWithIndex
          .map(track => Array((track._2 + releasesLastIndex + 1).toString) ++ track._1)
  }
}
