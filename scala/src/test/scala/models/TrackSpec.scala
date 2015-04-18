package models

import org.scalatest._

class TrackSpec extends FunSpec with Matchers {

  describe("hash") {
    it("returns the same hash for 2 tracks with same artists and same title") {
      val a = new Track(1,Array(2L),Array(3L),"Track Title",Array(5L))
      val b = new Track(2,Array(2L),Array(3L),"Track Title",Array(5L))

      assert(a.hash == b.hash)
    }

    it("return the same hash even if the title is not the same case") {
      val a = new Track(1,Array(2L),Array(3L),"Track Title",Array(5L))
      val b = new Track(2,Array(2L),Array(3L),"track title",Array(5L))

      assert(a.hash == b.hash)
    }
  }

  describe("toString") {
    it("returns the correct string") {
      val a = new Track(1,Array(33L,44L,55L),Array(3L,4L),"Track Title",Array()).toString
      val b = new Track(2,Array(33L,44L,55L),Array(3L,4L),"Track Title",Array(5L)).toString
      assert(a == "1\t33,44,55\t3,4\tTrack Title\t")
      assert(b == "2\t33,44,55\t3,4\tTrack Title\t5")
    }
  }

  describe("asNode") {
    it("returns the id, title and type") {
      val a = new Track(2,Array(33L,44L,55L),Array(3L,4L),"Track Title",Array(5L)).asNode
      assert(a == "2\tTrack Title\ttracktitle\tTrack")
    }
  }

  describe("addReleases") {
    it("adds a release to the list of releases") {
      var a = new Track(2,Array(33L,44L,55L),Array(3L,4L),"Track Title",Array(5L))
      val releases = Array(6666L,8888L)
      a = a.addReleases(releases)

      assert(a.releases.deep == Array(33L,44L, 55L, 6666L, 8888L).deep)
    }
  }

}
