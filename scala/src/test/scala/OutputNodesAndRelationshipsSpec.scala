package test

import main.OutputNodesAndRelationships
import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

class OutputNodesAndRelationshipsSpec extends FunSpec with Matchers {

  describe("extractArtistsReleasesRelationships") {
    it("joins the artists with the releases and extracts the relationships") {
      val sc = setupContext()
      try {
        val artistsRDD = OutputNodesAndRelationships.getArtists(sc.makeRDD(artists))
        val releasesRDD = OutputNodesAndRelationships.getReleases(sc.makeRDD(releases), artists.size)
        val result = OutputNodesAndRelationships.extractArtistsReleasesRelationships(artistsRDD, releasesRDD).collect

        assert(4 == result.size)
        assert(result.contains(List(0, 6, "HAS_TRACKLIST")))
        assert(result.contains(List(2, 5, "HAS_TRACKLIST")))
        assert(result.contains(List(2, 6, "HAS_TRACKLIST")))
        assert(result.contains(List(1, 4, "HAS_TRACKLIST")))

        } finally {
          sc.stop
        }
    }
  }

  describe("restructureRelease") {
    it("splits artists and moves fields around") {
      val input = (("rel1", "art3,art6"), 33L)
      assert(Array(("art3",("rel1", 33)), ("art6", ("rel1", 33))).deep == OutputNodesAndRelationships.restructureRelease(input).deep)
    }
  }

  describe("extractArtistReleaseRelationship") {
    it("returns the correct structure") {
      val input = ("art6", (33L, ("rel44", 9L)))
      assert(List(33, 9, "HAS_TRACKLIST") == OutputNodesAndRelationships.extractArtistReleaseRelationship(input))
    }
  }

  describe("getArtists") {
    it("returns the artists with index") {
      val sc = setupContext()
      try {
        val result = OutputNodesAndRelationships.getArtists(sc.makeRDD(artists())).collect
        assert((Array("art3", "name").deep, 0) == (result(0)._1.deep, result(0)._2))
        assert((Array("art5", "name").deep, 1) == (result(1)._1.deep, result(1)._2))
        assert((Array("art8", "name").deep, 2) == (result(2)._1.deep, result(2)._2))
        assert((Array("art9", "name").deep, 3) == (result(3)._1.deep, result(3)._2))
      } finally {
        sc.stop
      }
    }
  }

  describe("getReleases") {
    it("returns the releases with index") {
      val sc = setupContext()
      try {
        val result = OutputNodesAndRelationships.getReleases(sc.makeRDD(releases()), artists.size).collect
        assert((Array("rel1", "masterRel", "title", "art5").deep, 4) == (result(0)._1.deep, result(0)._2))
        assert((Array("rel3", "masterRel", "title", "art8").deep, 5) == (result(1)._1.deep, result(1)._2))
        assert((Array("rel88", "masterRel", "title", "art3,art8").deep, 6) == (result(2)._1.deep, result(2)._2))
        assert((Array("rel99", "masterRel", "title", "art4").deep, 7) == (result(3)._1.deep, result(3)._2))
      } finally {
        sc.stop
      }
    }
  }

  def artists(): Array[String] = {
    Array(
      "art3\tname",
      "art5\tname",
      "art8\tname",
      "art9\tname"
      )
  }

  def releases(): Array[String] = {
    Array(
      "rel1\tmasterRel\ttitle\tart5",
      "rel3\tmasterRel\ttitle\tart8",
      "rel88\tmasterRel\ttitle\tart3,art8",
      "rel99\tmasterRel\ttitle\tart4"
    )
  }

  def setupContext(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    new SparkContext("local[4]", "testing")
  }
}