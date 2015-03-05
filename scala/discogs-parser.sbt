name := "discogs-parser"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

parallelExecution in Test := false
