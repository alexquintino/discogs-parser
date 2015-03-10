package main

import models.Artist
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._

class NodeWriterSpec extends FunSpec with Matchers {
  describe("addIndex")  {
    it("keeps the index growing") {
      val sc = SparkUtil.context()
      try {
        val firstRun = NodeWriter.addIndex(nodes(sc)).collect()
        val secondRun = NodeWriter.addIndex(nodes(sc)).collect()
        val thirdRun = NodeWriter.addIndex(nodes(sc)).collect()
        assert((firstRun.head._2 + nodes(sc).count()) == secondRun.head._2)
        assert((firstRun.head._2 + nodes(sc).count()*2)== thirdRun.head._2)
      } finally {
        sc.stop()
      }
    }
  }

  def nodes(sc: SparkContext): RDD[Artist] = {
    sc.parallelize(List(
      new Artist("1","Artist1"),
      new Artist("2","Artist2"),
      new Artist("3","Artist3")
    ))
  }
}
