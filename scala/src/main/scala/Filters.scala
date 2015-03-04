import models.{Track, Artist}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Filters {
  def favoriteArtists(artists: RDD[Artist], favoriteArtistsNames: RDD[String]): RDD[Artist] = {
    val favoriteArtistsNamesWithNorm = favoriteArtistsNames.map(name => (Artist.normalize(name), name)) // (norm, name)
    favoriteArtistsNamesWithNorm.join(artists.map(artist => (artist.normalizedName, artist))).map(_._2._2)
  }

  def tracks(tracks: RDD[Track], artists: RDD[Artist]): RDD[Track] = {
    val artistsIds = artists.map(_.discogsId).collect().toSet[String]
    tracks.filter(track => contains(track.allArtists, artistsIds))
  }

  // checks if there's any of the values in a Set. Then reduces it to a single true/false
  def contains(values: Array[String], list: Set[String]): Boolean = {
    values.map(id => list.contains(id)).fold(false)((bool, res) => bool || res)
  }
}
