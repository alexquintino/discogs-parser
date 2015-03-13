package main

import java.io.File
import java.nio.charset.StandardCharsets
import sys.process._
import main.FileManager.Files

object MergeOutput {

  val ArtistHeaders = "i:id discogs_id:int name:string l:label\n"
  val TracklistHeaders = "i:id discogs_id:int title:string l:label\n"
  val TrackHeaders = "i:id title:string l:label\n"
  val RelationshipsHeader = "start end type\n"

  def mergeAll {
    mergeArtistNodes
    mergeTracklistNodes
    mergeTrackNodes
    mergeRelationships
  }
  def mergeArtistNodes {
    mergeNodes(ArtistHeaders, Files.ArtistNodes, Files.ArtistNodesTSV)
  }

  def mergeTracklistNodes {
    mergeNodes(TracklistHeaders, Files.TracklistNodes, Files.TracklistNodesTSV)
  }

  def mergeTrackNodes {
    mergeNodes(TrackHeaders, Files.TrackNodes, Files.TrackNodesTSV)
  }

  def mergeRelationships {
    val headersFile = writeToTempFile(RelationshipsHeader)
    val relationships = List(Files.ArtistReleaseRelationship, Files.ArtistTracksRelationship, Files.TracklistTrackRelationship, Files.RemixerTrackRelationship)
    val inputPaths = relationships.map(_.toString + "/part-*").mkString(" ")
    val outputPath = Files.RelationshipsTSV.toString
    mergeFiles(headersFile.getAbsolutePath, inputPaths, outputPath)
  }

  def mergeNodes(headers:String, input:Files.Value, output:Files.Value) {
    val headersFile = writeToTempFile(headers)
    val inputPath = s"${input.toString}/part-*"
    val outputPath = output.toString
    mergeFiles(headersFile.getAbsolutePath, inputPath, outputPath)
  }

  def mergeFiles(headersPath:String, inputPath:String, outputPath:String) {
    //needs sh because of wildcard
    val cmd = Seq("sh","-c",s"cat $headersPath $inputPath > $outputPath")
    val exitCode = cmd !;
    if(exitCode != 0) throw new RuntimeException("Error merging artist nodes")
  }

  def writeToTempFile(content: String):File = {
    val tempFile = File.createTempFile("nodes","csv")
    tempFile.deleteOnExit()
    java.nio.file.Files.write(tempFile.toPath, content.getBytes(StandardCharsets.UTF_8))
    tempFile
  }
}
