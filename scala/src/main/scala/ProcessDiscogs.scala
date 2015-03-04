import models.Artist
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProcessDiscogs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser")
    val sc = new SparkContext(conf)

    val artists = getArtists(sc)
    val favoriteArtists = filterFavoriteArtists(artists, getFavoriteArtistsNames(sc, args(0)))
  }

  def filterFavoriteArtists(artists: RDD[Artist], favoriteArtistsNames: RDD[String]): RDD[Artist] = {
    val favoriteArtistsNamesWithNorm = favoriteArtistsNames.map(name => (Artist.normalize(name), name)) // (norm, name)
    favoriteArtistsNamesWithNorm.join(artists.map(artist => (artist.normalizedName, artist))).map(_._2._2)
  }

  def getArtists(sc: SparkContext): RDD[Artist] = {
    sc.textFile("data/discogs_artists.csv").map(_.split("\t")).map { case fields:Array[String] => new Artist(fields(0), fields(1)) }
  }

  def getFavoriteArtistsNames(sc: SparkContext, file: String): RDD[String] = {
    sc.textFile(file)
  }
}
