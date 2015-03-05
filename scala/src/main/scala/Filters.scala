import models.{Release, Track, Artist}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Filters {

  def filterReleasesBasedOnTracks(releases: RDD[Release], tracks: RDD[Track]): RDD[Release] = {
    val releasesIds = tracks.map(_.release).collect.toSet
    releases.filter(rel => releasesIds.contains(rel.id))
  }

  def favoriteArtists(artists: RDD[Artist], favoriteArtistsNames: RDD[String]): RDD[Artist] = {
    val favoriteArtistsNamesWithNorm = favoriteArtistsNames.map(name => (Artist.normalize(name), name)) // (norm, name)
    favoriteArtistsNamesWithNorm.join(artists.map(artist => (artist.normalizedName, artist))).map(_._2._2)
  }

  def filterTracksBasedOnArtists(tracks: RDD[Track], artists: RDD[Artist]): RDD[Track] = {
    val artistsIds = artists.map(_.discogsId).collect().toSet[String]
    tracks.filter(track => contains(track.allArtists, artistsIds))
  }

  // go for the oldest release
  def filterReleasesBasedOnMasters(releases: RDD[Release]): RDD[Release] = {
    val releasesWithoutMaster = releases.filter(_.master.isEmpty)
    val releasesWithMaster = releases.filter(!_.master.isEmpty)
    val filteredReleases = releasesWithMaster.map(release => (release.master.toInt, release)).reduceByKey((rel1, rel2) => oldestRelease(rel1,rel2)).map(_._2)
    releasesWithoutMaster.union(filteredReleases)
  }

  // checks if there's any of the values in a Set. Then reduces it to a single true/false
  def contains(values: Array[String], list: Set[String]): Boolean = {
    values.map(id => list.contains(id)).fold(false)((bool, res) => bool || res)
  }

  def oldestRelease(release1: Release, release2: Release): Release = {
    val oldestId = Math.min(release1.id.toInt, release2.id.toInt)
    if(release1.id.toInt == oldestId) release1 else release2
  }
}
