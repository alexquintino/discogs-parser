import models.{Artist, Track}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.scalatest._

class FiltersSpec extends FunSpec with Matchers {
  it("filters out favorite artists") {
    val sc = setupContext()
    try {
      val result = Filters.favoriteArtists(artists(sc), favoriteArtistsNames(sc)).collect()
      assert(result.length == 2)
      assert(result.head.name == "Artist 2")
      assert(result.last.name == "Artist 4")
    } finally {
      sc.stop()
    }
  }

  it("filters out tracks based on a set of artists") {
    val sc = setupContext()
    try {
      val favArtists = Filters.favoriteArtists(artists(sc), favoriteArtistsNames(sc))
      val result = Filters.tracks(tracks(sc), favArtists).collect()

      assert(result.length == 2)
      assert(result.map(_.release).deep == Array("5", "8").deep)
    } finally {
      sc.stop()
    }
  }

  describe("contains") {
    it("checks if array has artists") {
      val artists_ids = Set("22","458","900","1234567")

      val artists1 = Array("22")
      val artists2 = Array("3","900")
      val artists3 = Array("901,12345678")

      val remixers = Array("458")

      assert(Filters.contains(artists1, artists_ids))
      assert(Filters.contains(artists2, artists_ids))
      assert(Filters.contains(artists2 ++ remixers, artists_ids))
      assert(!Filters.contains(artists3, artists_ids))
      assert(Filters.contains(artists3 ++ remixers, artists_ids))
      assert(!Filters.contains(Array(), artists_ids))
    }
  }

  def artists(sc:SparkContext) = {
    sc.parallelize(List(new Artist("1", "Artist 1"), new Artist("2", "Artist 2"), new Artist("3", "Artist 3"), new Artist("4", "Artist 4")))
  }

  def tracks(sc:SparkContext) = {
    sc.parallelize( List(
      new Track("0","1","Some", ""),
      new Track("2","3","Some name", "44"),
      new Track("5","6","Some other name", "4"),
      new Track("8","2","Some other other name", "10,11")
    ))
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
