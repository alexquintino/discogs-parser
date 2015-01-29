package test

import main.OutputNodesAndRelationships
import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

class OutputNodesAndRelationshipsSpec extends FunSpec with Matchers {

  describe("extracts") {
    describe("extractArtistsReleasesRelationships") {
      it("joins the artists with the releases and extracts the relationships") {
        val sc = setupContext()
        try {
          val artistsRDD = OutputNodesAndRelationships.getArtists(sc.makeRDD(artists))
          val releasesRDD = OutputNodesAndRelationships.getReleases(sc.makeRDD(releases), artists.size)
          val result = OutputNodesAndRelationships.extractArtistsReleasesRelationships(artistsRDD, releasesRDD).collect

          assert(4 == result.size)
          assert(result.contains(List("art3", "92", "HAS_TRACKLIST")))
          assert(result.contains(List("art8", "7", "HAS_TRACKLIST")))
          assert(result.contains(List("art8", "92", "HAS_TRACKLIST")))
          assert(result.contains(List("art5", "5", "HAS_TRACKLIST")))

          } finally {
            sc.stop
          }
      }
    }

    describe("extractReleasesTracksRelationships") {
      it("joins the releases with the tracks and extracts the relationships") {
        val sc = setupContext()
        try {
          val releasesRDD = OutputNodesAndRelationships.getReleases(sc.makeRDD(releases), artists.size)
          val tracksRDD = OutputNodesAndRelationships.getTracks(sc.makeRDD(tracks), releases.size, artists.size)
          val result = OutputNodesAndRelationships.extractReleasesTracksRelationships(releasesRDD, tracksRDD).collect

          assert(5 == result.size)
          assert(result.contains(List("7", 8, "HAS_TRACK")))
          assert(result.contains(List("103", 9, "HAS_TRACK")))
          assert(result.contains(List("5", 10, "HAS_TRACK")))
          assert(result.contains(List("5", 11, "HAS_TRACK")))
          assert(result.contains(List("92", 12, "HAS_TRACK")))

        } finally {
          sc.stop
        }
      }
    }

    describe("extractArtistsTracksRelationships") {
      it("joins the artists with tracks and extracts the relationships") {
        val sc = setupContext()
        try {
          val artistsRDD = OutputNodesAndRelationships.getArtists(sc.makeRDD(artists))
          val tracksRDD = OutputNodesAndRelationships.getTracks(sc.makeRDD(tracks), releases.size, artists.size)

          val result = OutputNodesAndRelationships.extractArtistsTracksRelationships(artistsRDD, tracksRDD).collect
          assert(7 == result.size)
          assert(result.contains(List("art3", 10, "HAS_TRACK")))
          assert(result.contains(List("art3", 12, "HAS_TRACK")))
          assert(result.contains(List("art5", 9, "HAS_TRACK")))
          assert(result.contains(List("art8", 8, "HAS_TRACK")))
          assert(result.contains(List("art9", 9, "HAS_TRACK")))
          assert(result.contains(List("art9", 11, "HAS_TRACK")))
          assert(result.contains(List("art9", 12, "HAS_TRACK")))
        } finally {
          sc.stop
        }
      }
    }
  }

  describe("restructureRelease") {
    it("splits artists and moves fields around") {
      val input = Array("33", "44", "master", "title", "art3,art6")
      assert(Array(("art3","33"), ("art6", "33")).deep == OutputNodesAndRelationships.restructureRelease(input).deep)
    }
  }

  describe("extractArtistReleaseRelationship") {
    it("returns the correct structure") {
      val input = ("art6", ("art6", "9"))
      assert(List("art6", "9", "HAS_TRACKLIST") == OutputNodesAndRelationships.extractArtistReleaseRelationship(input))
    }
  }

  describe("gets") {
    describe("getArtists") {
      it("returns the artists with id") {
        val sc = setupContext()
        try {
          val result = OutputNodesAndRelationships.getArtists(sc.makeRDD(artists())).collect
          assert(Array("art3", "art3", "name").deep == result(0).deep)
          assert(Array("art5", "art5", "name").deep == result(1).deep)
          assert(Array("art8", "art8", "name").deep == result(2).deep)
          assert(Array("art9", "art9", "name").deep == result(3).deep)
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
          assert(Array("5", "1", "masterRel", "title", "art5").deep == result(0).deep)
          assert(Array("7", "3", "masterRel", "title", "art8").deep == result(1).deep)
          assert(Array("92", "88", "masterRel", "title", "art3,art8").deep == result(2).deep)
          assert(Array("103", "99", "masterRel", "title", "art4").deep == result(3).deep)
        } finally {
          sc.stop
        }
      }
    }

    describe("getTracks") {
      it("returns the tracks with index") {
        val sc = setupContext()
        try {
          val result = OutputNodesAndRelationships.getTracks(sc.makeRDD(tracks), artists.size, releases.size).collect
          assert((Array("3", "art8", "title1").deep, 8) == (result(0)._1.deep, result(0)._2))
          assert((Array("99", "art5,art9", "title2").deep, 9) == (result(1)._1.deep, result(1)._2))
          assert((Array("1", "art3", "title3").deep, 10) == (result(2)._1.deep, result(2)._2))
          assert((Array("1", "art9", "title4").deep, 11) == (result(3)._1.deep, result(3)._2))
          assert((Array("88", "art9,art3", "title5").deep, 12) == (result(4)._1.deep, result(4)._2))
        } finally {
          sc.stop
        }
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
      "1\tmasterRel\ttitle\tart5",
      "3\tmasterRel\ttitle\tart8",
      "88\tmasterRel\ttitle\tart3,art8",
      "99\tmasterRel\ttitle\tart4"
    )
  }

  def tracks(): Array[String] = {
    Array(
      "3\tart8\ttitle1",
      "99\tart5,art9\ttitle2",
      "1\tart3\ttitle3",
      "1\tart9\ttitle4",
      "88\tart9,art3\ttitle5"
      )
  }

  def setupContext(): SparkContext = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    new SparkContext("local[8]", "testing")
  }
}
