package main

import main.deduplication.TrackDeduplicator
import models.Track
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{Matchers, FunSpec}

class TrackDeduplicatorSpec extends FunSpec with Matchers{

  describe("deduplicate") {
    it("returns a list without duplicates") {
      val sc = SparkUtil.context()
      try {
        val dedupTracks = TrackDeduplicator.deduplicate(tracks(sc)).collect().toList.map(_.id)

        assert(dedupTracks.length == 3)
        assert(dedupTracks.contains("id1"))
        assert(dedupTracks.contains("id2"))
        assert(dedupTracks.contains("id4"))
      } finally {
        sc.stop()
      }
    }
  }

  def tracks(sc:SparkContext): RDD[Track] = {
    sc.parallelize(List(
      new Track("id1","rel1","art8","title1",""),
      new Track("id2","rel2","art5,art9","title2","art8"),
      new Track("id3","rel3","art8","title1",""),
      new Track("id4","rel4","art5","title3","art3"),
      new Track("id5","rel5","art5,art9","TitLe2","art8")
    ))
  }
}
