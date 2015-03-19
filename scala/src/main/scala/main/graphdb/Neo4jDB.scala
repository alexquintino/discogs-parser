package main.graphdb

import main.util.FileManager
import main.util.FileManager.Files._

import scala.sys.process._

object Neo4jDB {

  val Db = "graph.db"
  val BatchImportPath = "/opt/batch_importer_21"

  def create {
    val nodes = List(ArtistNodesTSV, TracklistNodesTSV, TrackNodesTSV).mkString(",")
    val exitCode = Seq("sh","-c", s"cd $BatchImportPath && ./import.sh $Db $nodes $RelationshipsTSV").!
    if(exitCode != 0) throw new RuntimeException("Failed to batch import the nodes and relationships into Neo4j DB")
  }
}
