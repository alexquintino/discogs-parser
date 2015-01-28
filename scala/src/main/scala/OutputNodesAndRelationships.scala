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
    val artistsCount = artists.count()
    savaArtistsNodes(artists)


    // release_id / master_id / title / main_artists
    val releases = getReleases(sc.textFile("output/releases", 1), artistsCount)
    val releasesCount = releases.count()
    saveReleasesNodes(releases)

    // release_id / artists / title / remixers
    val tracks = getTracks(sc.textFile("output/tracks"), artistsCount, releasesCount)
    val tracksCount = tracks.count()
    saveTracksNodes(tracks)


    // Relationships
    extractArtistsReleasesRelationships(artists, releases)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/artist_release_relationship")

    extractReleasesTracksRelationships(releases, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/tracklist_tracks_relationship")

    extractArtistsTracksRelationships(artists, tracks)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/artist_tracks_relationship")


  }


  def extractArtistsReleasesRelationships(artists: RDD[(Array[String], Long)], releases: RDD[(Array[String], Long)]): RDD[List[Any]] = {
    val artistsMap = artists.map(artist => (artist._1(0), artist._2))
    val releasesMap =  releases.map(rel => ((rel._1(0), rel._1(3)), rel._2)).flatMap(restructureRelease)
    artistsMap.join(releasesMap)
              .map(extractArtistReleaseRelationship)
  }

  def extractReleasesTracksRelationships(releases: RDD[(Array[String], Long)], tracks: RDD[(Array[String], Long)]): RDD[List[Any]] = {
    val releasesMap = releases.map(rel => (rel._1(0), rel._2))
    val tracksMap = tracks.map(track => (track._1(0), track._2))
    releasesMap.join(tracksMap)
                .map(extractReleaseTrackRelationship)
  }

  def extractArtistsTracksRelationships(artists: RDD[(Array[String], Long)], tracks: RDD[(Array[String], Long)]): RDD[List[Any]] = {
    val artistsMap = artists.map(artist => (artist._1(0), artist._2))
    val tracksMap = tracks.map(track => (track._1(1), track._2)).flatMap(splitArtistsInTrack)
    artistsMap.join(tracksMap)
              .map(extractArtistTrackRelationship)
  }

  def extractArtistReleaseRelationship(rel: (String, (Long, (String, Long)))): List[Any] = {
    List(rel._2._1, rel._2._2._2, "HAS_TRACKLIST")
  }

  def extractReleaseTrackRelationship(rel: (String, (Long, Long))): List[Any] = {
    List(rel._2._1, rel._2._2, "HAS_TRACK")
  }

  def extractArtistTrackRelationship(rel: (String, (Long, Long))): List[Any] = {
    List(rel._2._1, rel._2._2, "HAS_TRACK")
  }

  def restructureRelease(release: ((String, String), Long)): Array[(String, (String, Long))] = {
    val artists = release._1._2
    artists.split(",").map{
      artist => (artist, (release._1._1, release._2)) //from ((releaseId, artists), index) to (artistId, (releaseId, index))
    }
  }

  def splitArtistsInTrack(track: (String, Long)): Array[(String, Long)] = {
    val artists = track._1
    artists.split(",").map {
      artist => (artist, track._2)
    }
  }



  def savaArtistsNodes(artists: RDD[(Array[String], Long)]) {
    artists.map(artist => artist._1 ++ Array("Artist")).map(_.mkString("\t")).saveAsTextFile("output/artists_nodes")
  }

  def saveReleasesNodes(releases: RDD[(Array[String], Long)]) {
    releases.map(release => Array(release._1(0), release._1(2), "Tracklist").mkString("\t")).saveAsTextFile("output/tracklists_nodes")
  }

  def saveTracksNodes(tracks: RDD[(Array[String], Long)]) {
    tracks.map(track => Array(track._1(2), "Track").mkString("\t")).saveAsTextFile("output/tracks_nodes")
  }

  def getArtists(artists: RDD[String]): RDD[(Array[String], Long)] = { artists.map(_.split("\t")).zipWithIndex }
  def getReleases(releases: RDD[String], artistsCount: Long): RDD[(Array[String], Long)] = {
    releases.map(_.split("\t"))
            .zipWithIndex
            .map(release => (release._1, release._2 + artistsCount))
  }
  def getTracks(tracks: RDD[String], artistsCount: Long, releasesCount: Long ): RDD[(Array[String], Long)] = {
    tracks.map(_.split("\t"))
          .zipWithIndex
          .map(track => (track._1, track._2 + artistsCount + releasesCount))
  }
}
