import models.{Track, Artist}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ProcessDiscogs {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser")
    val sc = new SparkContext(conf)

    var artists = getArtists(sc)
    artists = Filters.favoriteArtists(artists, getFavoriteArtistsNames(sc, args(0)))

    val tracks = getTracks(sc)
    val filteredTracks = Filters.tracks(tracks, artists)

  }

  def getArtists(sc: SparkContext): RDD[Artist] = {
    sc.textFile("data/discogs_artists.csv").map(_.split("\t")).map { case fields:Array[String] => new Artist(fields(0), fields(1)) }
  }

  def getFavoriteArtistsNames(sc: SparkContext, file: String): RDD[String] = {
    sc.textFile(file)
  }

  def getTracks(sc: SparkContext): RDD[Track] = {
    sc.textFile("output/discogs_tracks.tsv").map(_.split("\t")).filter(_.size > 2).map {
      fields => if (fields.length == 3)
        new Track(fields(0), fields(1), fields(2), "")
      else
        new Track(fields(0), fields(1), fields(2), fields(3))
    }
  }
}
