package test

import models.Track
import org.scalatest._

class TrackSpec extends FunSpec with Matchers {

  describe("hashCode") {
    it("returns the same hashCode for 2 tracks with same artists and same title") {
      val a = new Track("id1","release1","art2","Track Tile","remixer3")
      val b = new Track("id2","release2","art2","Track Tile","remixer3")

      assert(a.hashCode == b.hashCode)
    }

    it("return the same hashCode even if the title is not the same case") {
      val a = new Track("id1","release1","art2","Track Tile","remixer3")
      val b = new Track("id2","release2","art2","track tile","remixer3")

      assert(a.hashCode == b.hashCode)
    }
  }

}
