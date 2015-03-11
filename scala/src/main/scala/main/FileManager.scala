package main

import sys.process._

object FileManager {
  object Files extends Enumeration {
    val DiscogsArtists = Value("output/discogs_artists.tsv")
    val DiscogsReleases = Value("output/discogs_releases.tsv")
    val DiscogsTracks = Value("output/discogs_tracks.tsv")

    val ArtistTracksRelationship = Value("output/artist_track_relationships")
    val ArtistReleaseRelationship = Value("output/artist_release_relationships")
    val TracklistTrackRelationship = Value("output/tracklist_track_relationships")
    val RemixerTrackRelationship = Value("output/remixer_track_relationship")

    val ArtistNodes = Value("output/artist_nodes")
    val TracklistNodes = Value("output/tracklist_nodes")
    val TrackNodes = Value("output/track_nodes")

    def forNodes(nodeType:String): String = {
      Files.withName(s"output/${nodeType.toLowerCase}_nodes").toString
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
