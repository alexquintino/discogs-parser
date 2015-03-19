package main

import main.util.Relationships
import models.{Track, Artist, Release}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._

class RelationshipsSpec extends FunSpec with Matchers {

  describe("extracts") {
    describe("extractArtistsReleasesRelationships") {
      it("joins the artists with the releases and extracts the relationships") {
        val sc = SparkUtil.context()
        try {
          val result = Relationships.extractArtistsReleasesRelationships(artists(sc), releases(sc)).collect

          assert(4 == result.size)
          assert(result.contains(List("3", "97", "HAS_TRACKLIST")))
          assert(result.contains(List("8", "12", "HAS_TRACKLIST")))
          assert(result.contains(List("8", "97", "HAS_TRACKLIST")))
          assert(result.contains(List("5", "10", "HAS_TRACKLIST")))
        } finally {
          sc.stop
        }
      }
    }

    describe("extractReleasesTracksRelationships") {
      it("joins the releases with the tracks and extracts the relationships") {
        val sc = SparkUtil.context()
        try {
          val result = Relationships.extractReleasesTracksRelationships(releases(sc), tracks(sc)).collect

          assert(5 == result.size)
          assert(result.contains(List("12", "109", "HAS_TRACK")))
          assert(result.contains(List("108", "110", "HAS_TRACK")))
          assert(result.contains(List("10", "111", "HAS_TRACK")))
          assert(result.contains(List("10", "112", "HAS_TRACK")))
          assert(result.contains(List("97", "113", "HAS_TRACK")))

        } finally {
          sc.stop
        }
      }
    }
    describe("extractArtistsTracksRelationships") {
      it("joins the artists with tracks and extracts the relationships") {
        val sc = SparkUtil.context()
        try {
          val result = Relationships.extractArtistsTracksRelationships(artists(sc), tracks(sc)).collect
          assert(7 == result.size)
          assert(result.contains(List("3", "111", "HAS_TRACK")))
          assert(result.contains(List("3", "113", "HAS_TRACK")))
          assert(result.contains(List("5", "110", "HAS_TRACK")))
          assert(result.contains(List("8", "109", "HAS_TRACK")))
          assert(result.contains(List("9", "110", "HAS_TRACK")))
          assert(result.contains(List("9", "112", "HAS_TRACK")))
          assert(result.contains(List("9", "113", "HAS_TRACK")))
        } finally {
          sc.stop
        }
      }
    }

    describe("extractRemixersTracksRelationships") {
      it("joins the remixers with tracks and extracts the relationships") {
        val sc = SparkUtil.context()
        try {
          val result = Relationships.extractRemixersTracksRelationships(artists(sc), tracks(sc)).collect
          assert(2 == result.size)
          assert(result.contains(List("8", "110", "HAS_REMIX")))
          assert(result.contains(List("3", "112", "HAS_REMIX")))
        } finally {
          sc.stop
        }
      }
    }
  }

  describe("restructureRelease") {
    it("splits artists and moves fields around") {
      val input = (new Release("44", "master", "title", "art3,art6"), 33L)
      assert(Array(("art3","33"), ("art6", "33")).deep == Relationships.restructureRelease(input).deep)
    }
  }

  describe("restructureTrack") {
    it("splits the releases into several lines") {
      val results = Relationships.restructureTrack(new Track("id",Array(3,6,8),Array(1),"title",Array()), 44L)
      assert(results.length == 3)
      assert(results.contains(("3","44")))
      assert(results.contains(("6","44")))
      assert(results.contains(("8","44")))
    }
  }

  describe("extractArtistReleaseRelationship") {
    it("returns the correct structure") {
      val input = ("art6", ("art6", "9"))
      assert(List("art6", "9", "HAS_TRACKLIST") == Relationships.extractArtistReleaseRelationship(input))
    }
  }


  def artists(sc:SparkContext): RDD[(Artist, Long)] = {
    sc.parallelize(List(
      (new Artist("3","name"), 3L),
      (new Artist("5","name"), 5L),
      (new Artist("8","name"), 8L),
      (new Artist("9","name"), 9L)
    ))
  }

  def releases(sc:SparkContext): RDD[(Release, Long)] = {
    sc.parallelize(List(
      (new Release("1","masterRel","title","5"), 10L),
      (new Release("3","masterRel","title","8"), 12L),
      (new Release("88","masterRel","title","3,8"), 97L),
      (new Release("99","masterRel","title","4"), 108L)
    ))
  }

  def tracks(sc:SparkContext): RDD[(Track, Long)] = {
    sc.parallelize(List(
      (new Track("1",Array(3),Array(8),"title1",Array()), 109L),
      (new Track("2",Array(99),Array(5,9),"title2",Array(8)), 110L),
      (new Track("3",Array(1),Array(3),"title3",Array()), 111L),
      (new Track("4",Array(1),Array(9),"title4",Array(3)), 112L),
      (new Track("5",Array(88),Array(9,3),"title5",Array()), 113L)
    ))
  }
}
