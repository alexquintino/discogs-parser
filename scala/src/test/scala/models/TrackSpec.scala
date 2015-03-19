package models

import org.scalatest._

class TrackSpec extends FunSpec with Matchers {

  describe("hash") {
    it("returns the same hashCode for 2 tracks with same artists and same title") {
      val a = new Track(1,Array(2L),Array(3L),"Track Tile",Array(5L))
      val b = new Track(2,Array(2L),Array(3L),"Track Tile",Array(5L))

      assert(a.hash == b.hash)
    }

    it("return the same hashCode even if the title is not the same case") {
      val a = new Track(1,Array(2L),Array(3L),"Track Tile",Array(5L))
      val b = new Track(2,Array(2L),Array(3L),"track tile",Array(5L))

      assert(a.hash == b.hash)
    }
  }

  describe("toString") {
    it("returns the correct string") {
      val a = new Track(1,Array(33L,44L,55L),Array(3L,4L),"Track Tile",Array()).toString
      val b = new Track(2,Array(33L,44L,55L),Array(3L,4L),"Track Tile",Array(5L)).toString
      assert(a == "1\t33,44,55\t3,4\tTrack Tile\t")
      assert(b == "2\t33,44,55\t3,4\tTrack Tile\t5")
    }
  }

  describe("asNode") {
    it("returns the id, title and type") {
      val a = new Track(2,Array(33L,44L,55L),Array(3L,4L),"Track Tile",Array(5L)).asNode
      assert(a == "2\tTrack Tile\tTrack")
    }
  }

  describe("addReleases") {
    it("adds a release to the list of releases") {
      var a = new Track(2,Array(33L,44L,55L),Array(3L,4L),"Track Tile",Array(5L))
      val releases = Array(6666L,8888L)
      a = a.addReleases(releases)

      assert(a.releases.deep == Array(33L,44L, 55L, 6666L, 8888L).deep)
    }
  }

}
