import models.Node
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object NodeWriter {
  var lastIndex = 0L

  def writeNodes[T](nodes: RDD[T], nodeType: String) {
    val nodesWithIndex = addIndex(nodes)
    nodesWithIndex.map { case (node:Node, index) => s"${index}\t${node.asNode}" }.saveAsTextFile(s"${nodeType}_nodes")
  }

  def addIndex[T](nodes: RDD[T]): RDD[(T with Node, Long)] = {
    val withIndex = nodes.map { case node:T with Node => (node, node.id.toLong + lastIndex) }
    lastIndex = withIndex.map(_._2).max
    withIndex
  }
}
