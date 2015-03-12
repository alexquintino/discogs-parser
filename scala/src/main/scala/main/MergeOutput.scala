package main

import java.io.File
import java.nio.charset.StandardCharsets
import sys.process._
import main.FileManager.Files

object MergeOutput {

  def mergeArtistNodes {
    mergeNodes(artistHeaders, Files.ArtistNodes, Files.ArtistNodesTSV)
  }

  def mergeTracklistNodes {
    mergeNodes(tracklistHeaders, Files.TracklistNodes, Files.TracklistNodesTSV)
  }

  def mergeTrackNodes {
    mergeNodes(tracklistHeaders, Files.TracklistNodes, Files.TracklistNodesTSV)
  }

  def mergeRelationships {
    val headersFile = writeToTempFile(relationshipsHeader)
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
    val cmd = s"cat $headersPath $inputPath > $outputPath"
    val exitCode = cmd.!
    if(exitCode != 0) throw new RuntimeException("Error merging artist nodes")
  }

  def artistHeaders = "i:id discogs_id:int name:string l:label"
  def tracklistHeaders = "i:id discogs_id:int title:string l:label"
  def trackHeaders = "i:id title:string l:label"
  def relationshipsHeader = "start end type"

  def writeToTempFile(content: String):File = {
    val tempFile = File.createTempFile("nodes","csv")
    tempFile.deleteOnExit()
    java.nio.file.Files.write(tempFile.toPath, content.getBytes(StandardCharsets.UTF_8))
    tempFile
  }
}
