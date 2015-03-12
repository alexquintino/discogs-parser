package main

import sys.process._

object FileManager {
  val outputPath = sys.env("DISCOGSPARSER_OUTPUT_PATH")

  object Files extends Enumeration {
    val DiscogsArtists = Value(s"$outputPath/discogs_artists.tsv")
    val DiscogsReleases = Value(s"$outputPath/discogs_releases.tsv")
    val DiscogsTracks = Value(s"$outputPath/discogs_tracks.tsv")

    val ArtistTracksRelationship = Value(s"$outputPath/artist_track_relationships")
    val ArtistReleaseRelationship = Value(s"$outputPath/artist_release_relationships")
    val TracklistTrackRelationship = Value(s"$outputPath/tracklist_track_relationships")
    val RemixerTrackRelationship = Value(s"$outputPath/remixer_track_relationship")

    val ArtistNodes = Value(s"$outputPath/artist_nodes")
    val TracklistNodes = Value(s"$outputPath/tracklist_nodes")
    val TrackNodes = Value(s"$outputPath/track_nodes")

    val ArtistNodesTSV = Value("output/artist_nodes.tsv")
    val TracklistNodesTSV = Value("output/tracklist_nodes.tsv")
    val TrackNodesTSV = Value("output/track_nodes.tsv")
    val RelationshipsTSV = Value("output/relationships.tsv")

    def forNodes(nodeType:String): String = {
      Files.withName(s"$outputPath/${nodeType.toLowerCase}_nodes").toString
    }
  }

  def cleanup {
    outputs.foreach(delete)
  }

  private
  def delete(path:Files.Value) = s"rm -r ${path.toString}".!
  def outputs = List(
    Files.ArtistReleaseRelationship,
    Files.ArtistTracksRelationship,
    Files.TracklistTrackRelationship,
    Files.RemixerTrackRelationship,
    Files.ArtistNodes,
    Files.TracklistNodes,
    Files.TrackNodes)
}
