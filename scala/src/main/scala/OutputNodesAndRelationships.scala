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

    // format: (artist_id, (artist_index, (release_id, release_index)))
    extractArtistsReleasesRelationships(artists, releases)
      .map(_.mkString("\t"))
      .saveAsTextFile("output/artist_release_relationship")


    // val tracksMap = makeTracksMap(tracks, artistsCount + releasesCount)
  }


  def extractArtistsReleasesRelationships(artists: RDD[(Array[String], Long)], releases: RDD[(Array[String], Long)]): RDD[List[Any]] = {
    val artistsMap = artists.map(artist => (artist._1(0), artist._2))
    val releasesMap =  releases.map(rel => ((rel._1(0), rel._1(3)), rel._2)).flatMap(restructureRelease)
    artistsMap.join(releasesMap)
              .map(extractArtistReleaseRelationship)
  }

  def extractArtistReleaseRelationship(rel: (String, (Long, (String, Long)))): List[Any] = {
    List(rel._2._1, rel._2._2._2, "HAS_TRACKLIST")
  }

  // //return (artist_id, (release_id, release_index))
  // def makeReleasesMap(releases: RDD[Array[String]], artistsCount: Long): RDD[(String, (String, Long))] = {
  //   releases.map(release => (release(0), release(3))).zipWithIndex()
  //     .map(rel => (rel._1, rel._2 + artistsCount)) //adjust releases index with artists count
  //     .flatMap(restructureRelease)
  // }

  def restructureRelease(release: ((String, String), Long)): Array[(String, (String, Long))] = {
    val artists = release._1._2
    artists.split(",").map{
      artist => (artist, (release._1._1, release._2)) //from ((releaseId, artists), index) to (artistId, (releaseId, index))
    }
  }

  // def joinReleasesTracks(releases: RDD[Array[String], tracks: RDD[Array[String]]])

  // def makeTracksMap(tracks: RDD[Array[String]], artistsPlusReleasesCount: Long): RDD[(String, Long)] = {
  //   tracks.map(track => track(0)).zipWithIndex().map(track => (track._1, track._2 + artistsPlusReleasesCount))
  // }

  def savaArtistsNodes(artists: RDD[(Array[String], Long)]) {
    artists.map(artist => artist._1 ++ Array("Artist")).map(_.mkString("\t")).saveAsTextFile("output/artists_nodes")
  }

  def saveReleasesNodes(releases: RDD[(Array[String], Long)]) {
    releases.map(release => Array(release._1(0), release._1(2)).mkString("\t")).saveAsTextFile("output/tracklist_nodes")
  }

  def saveTracksNodes(tracks: RDD[(Array[String], Long)]) {
    tracks.map(track => track._1(2)).saveAsTextFile("output/track_nodes")
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
