import main.FilterArtistsAndReleases
import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

class FilterArtistsAndReleasesSpec extends FunSpec with Matchers {

  describe("containsArtists") {
    it("checks if array has artists") {
      val artists_ids = Set("22","458","900","1234567")

      val artists1 = Array("22")
      val artists2 = Array("3","900")
      val artists3 = Array("901,12345678")

      val remixers = Array("458")

      assert(FilterArtistsAndReleases.containsArtists(artists1, artists_ids))
      assert(FilterArtistsAndReleases.containsArtists(artists2, artists_ids))
      assert(FilterArtistsAndReleases.containsArtists(artists2 ++ remixers, artists_ids))
      assert(!FilterArtistsAndReleases.containsArtists(artists3, artists_ids))
      assert(FilterArtistsAndReleases.containsArtists(artists3 ++ remixers, artists_ids))
      assert(!FilterArtistsAndReleases.containsArtists(Array(), artists_ids))
    }
  }

  describe("trackArtists") {
    it("returns the artist when there is no remixer") {
      assert(FilterArtistsAndReleases.trackArtists(Array("1","2","Some name")).deep == Array("2").deep)
    }

    it("returns the artist and remixer when there is one") {
      assert(FilterArtistsAndReleases.trackArtists(Array("1","2","Some name","3")).deep == Array("2","3").deep)
    }
  }

  describe("grabTracksForArtists") {
    it("grabs the tracks for the list of artists") {
      val sc = setupContext()
      try {
        val tracksRDD = sc.makeRDD(tracks())
        val artists1 = Set("33")
        val artists2 = Set("77")
        val artists3 = Set("99","10","66")

        val result1 = FilterArtistsAndReleases.grabTracksForArtists(tracksRDD, artists1)
        val result2 = FilterArtistsAndReleases.grabTracksForArtists(tracksRDD, artists2)
        val result3 = FilterArtistsAndReleases.grabTracksForArtists(tracksRDD, artists3)

        assert(result1.count == 1)
        assert(result1.collect.head.deep == tracks().head.deep)

        assert(result2.count == 1)
        assert(result2.collect.head.deep == tracks()(1).deep)

        assert(result3.count == 2)
        assert(result3.collect.head.deep == tracks()(1).deep)
        assert(result3.collect()(1).deep == tracks()(2).deep)
      }
      finally {
        sc.stop
      }
    }
  }

  describe("isMainRelease") {
    val mainReleases = Set("99","88","77")

    it("returns true when it doesn't have a master") {
      val release = Array("12", "", "Some name", "11,22")
      assert(FilterArtistsAndReleases.isMainRelease(release, mainReleases))
    }

    it("returns false when it has a master and is not part of the main releases") {
      val release = Array("98", "13", "Some Other name", "11")
      assert(!FilterArtistsAndReleases.isMainRelease(release, mainReleases))
    }

    it("returns true when it has a master and it is part of the main releases") {
      val release = Array("88", "13", "Some Other name", "11")
      assert(FilterArtistsAndReleases.isMainRelease(release, mainReleases))
    }
  }

  def tracks(): Array[Array[String]] = {
    Array(
      Array("2","33","Some name", "44"),
      Array("5","66","Some other name", "77"),
      Array("8","99","Some other other name", "10,11")
    )
  }

  def setupContext(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    new SparkContext("local[4]", "testing")
  }

}
