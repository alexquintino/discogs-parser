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
        assert(dedupTracks.contains(1))
        assert(dedupTracks.contains(2))
        assert(dedupTracks.contains(4))
      } finally {
        sc.stop()
      }
    }
  }

  def tracks(sc:SparkContext): RDD[Track] = {
    sc.parallelize(List(
      new Track(1,Array(1),Array(8),"title1",Array()),
      new Track(2,Array(2),Array(5,9),"title2",Array(8)),
      new Track(3,Array(3),Array(8),"title1",Array()),
      new Track(4,Array(4),Array(5),"title3",Array(3)),
      new Track(5,Array(5),Array(5,9),"TitLe2",Array(8))
    ))
  }
}
