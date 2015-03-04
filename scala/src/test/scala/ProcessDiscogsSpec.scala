import models.Artist
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.scalatest._

class ProcessDiscogsSpec extends FunSpec with Matchers {

  it("filters out favorite artists") {
    val sc = setupContext()
    try {
      val result = ProcessDiscogs.filterFavoriteArtists(artists(sc), favoriteArtistsNames(sc)).collect
      assert(result.length == 2)
      assert(result.head.name == "Artist 2")
      assert(result.last.name == "Artist 4")
    } finally {
      sc.stop()
    }
  }

  def artists(sc:SparkContext) = {
    sc.parallelize(List(new Artist("1", "Artist 1"), new Artist("2", "Artist 2"), new Artist("3", "Artist 3"), new Artist("4", "Artist 4")))
  }

  def favoriteArtistsNames(sc:SparkContext) = {
    sc.parallelize(List("artist 2", "Artist 4"))
  }

  def setupContext(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    new SparkContext("local[8]", "testing")
  }
}
