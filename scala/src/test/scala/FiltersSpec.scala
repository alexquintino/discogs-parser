import models.{Release, Artist, Track}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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
      val result = Filters.filterTracksBasedOnArtists(tracks(sc), favArtists).collect()

      assert(result.length == 3)
      assert(result.map(_.release).deep == Array("5", "8", "10").deep)
    } finally {
      sc.stop()
    }
  }

  it("filters out releases based on tracks") {
    val sc = setupContext()
    try {
      val filteredTracks = Filters.filterTracksBasedOnArtists(tracks(sc), Filters.favoriteArtists(artists(sc), favoriteArtistsNames(sc)))
      val result = Filters.filterReleasesBasedOnTracks(releases(sc), filteredTracks).collect()

      assert(result.length == 3)
      assert(result.map(_.id).deep == Array("5", "8", "10").deep)
      assert(result.map(_.master).deep == Array("66", "66", "").deep)
    } finally {
      sc.stop()
    }
  }

  it("filters out releases based on the master") {
    val sc = setupContext()
    try {
      val filteredReleases = Filters.filterReleasesBasedOnTracks(releases(sc), Filters.filterTracksBasedOnArtists(tracks(sc), Filters.favoriteArtists(artists(sc), favoriteArtistsNames(sc))))
      val result = Filters.filterReleasesBasedOnMasters(filteredReleases).collect()

      assert(result.length == 2)
      assert(result.map(_.id).deep == Array("10", "5").deep)
      assert(result.map(_.master).deep == Array("", "66").deep)
    } finally {
      sc.stop()
    }
  }

  it("filters tracks based on a list of releases") {
    val sc = setupContext()
    try {
      val selectedReleases = releases(sc).filter(r => r.id == "2" || r.id == "8")
      val result = Filters.filterTracksBasedOnReleases(tracks(sc), selectedReleases).collect()

      assert(result.length == 2)
      assert(result.map(_.release).deep == Array("2", "8").deep)
      assert(result.map(_.title).deep == Array("Some name", "Some other other name").deep)
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

  describe("oldestRelease") {
    it("returns the oldestRelease correctly") {
      val release1 = new Release("99", "33", "Release1", "3")
      val release2 = new Release("999", "33", "Release1", "3")
      assert(Filters.oldestRelease(release1, release2) == release1)
    }
  }

  def artists(sc:SparkContext) = {
    sc.parallelize(List(
      new Artist("1", "Artist 1"),
      new Artist("2", "Artist 2"),
      new Artist("3", "Artist 3"),
      new Artist("4", "Artist 4")))
  }

  def tracks(sc:SparkContext) = {
    sc.parallelize( List(
      new Track("0","1","Some", ""),
      new Track("2","3","Some name", "44"),
      new Track("5","6","Some other name", "4"),
      new Track("8","2","Some other other name", "10,11"),
      new Track("10","4","Some other other other name", "")))
  }

  def releases(sc:SparkContext) = {
    sc.parallelize(List(
      new Release("1", "33", "Title1", "2,3"),
      new Release("2", "44", "Title2", "4"),
      new Release("3", "55", "Title3", "1"),
      new Release("4", "33", "Title1", "2,3"),
      new Release("5", "66", "Title5", "10,11"),
      new Release("8", "66", "Title5", "10,11"),
      new Release("10", "", "Title10", "4")
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
