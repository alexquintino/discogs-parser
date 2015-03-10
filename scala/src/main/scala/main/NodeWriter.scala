package main

import models.Node
import org.apache.spark.rdd.RDD

object NodeWriter {
  var lastIndex = 0L

  def writeNodes[T <: Node](nodes: RDD[T], nodeType: String): RDD[(T, Long)] = {
    val nodesWithIndex = addIndex(nodes)
    nodesWithIndex.map { case (node, index) => s"${index}\t${node.asNode}" }.saveAsTextFile(s"output/${nodeType}_nodes")
    nodesWithIndex
  }

  def addIndex[T <: Node](nodes: RDD[T]): RDD[(T, Long)] = {
    val withIndex = nodes.map { case node => (node, node.id.toLong + lastIndex) }
    lastIndex = withIndex.map(_._2).max
    withIndex
  }
}