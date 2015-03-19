package main

import main.graphdb.{Neo4jDB, MergeOutput}

object CreateDB {
  def main(args: Array[String]) {
    MergeOutput.mergeAll
    Neo4jDB.create
  }
}
