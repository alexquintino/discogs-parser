package models

import org.scalatest._

class TrackSpec extends FunSpec with Matchers {

  describe("hash") {
    it("returns the same hashCode for 2 tracks with same artists and same title") {
      val a = new Track("id1","release1","art2","Track Tile","remixer3")
      val b = new Track("id2","release2","art2","Track Tile","remixer3")

      assert(a.hash == b.hash)
    }

    it("return the same hashCode even if the title is not the same case") {
      val a = new Track("id1","release1","art2","Track Tile","remixer3")
      val b = new Track("id2","release2","art2","track tile","remixer3")

      assert(a.hash == b.hash)
    }
  }

  describe("toString") {
    it("returns the correct string") {
      val a  = new Track("id1", "1111,2222,3333,4444", "44,55,66", "title1","").toString
      val b  = new Track("id1", "1111,2222,3333,4444", "44,55,66", "title1","999").toString
      assert(a == "id1\t1111,2222,3333,4444\t44,55,66\ttitle1\t")
      assert(b == "id1\t1111,2222,3333,4444\t44,55,66\ttitle1\t999")
    }
  }

  describe("asNode") {
    it("returns the id, title and type") {
      val a  = new Track("id1", "1,2,3,4", "44,55,66", "title1","").asNode
      assert(a == "id1\ttitle1\tTrack")
    }
  }

  describe("addReleases") {
    it("adds a release to the list of releases") {
      var a = new Track("id1", "1111,2222,3333,4444", "44,55,66", "title1","")
      val releases = Array("6666","8888")
      a = a.addReleases(releases)

      assert(a.releases.deep == Array("1111","2222","3333","4444","6666","8888").deep)
    }
  }

}
