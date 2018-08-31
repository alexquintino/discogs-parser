package main.util

object FileManager {
  val dataPath = s"${sys.env("DATA_PATH")}/discogs"
  val intermediateOutputs = List(
    Files.ArtistReleaseRelationship,
    Files.ArtistTracksRelationship,
    Files.TracklistTrackRelationship,
    Files.RemixerTrackRelationship,
    Files.ArtistNodes,
    Files.TracklistNodes,
    Files.TrackNodes)

  object Files extends Enumeration {
    val DiscogsArtists = Value(s"$dataPath/data/discogs_artists.tsv")
    val DiscogsReleases = Value(s"$dataPath/data/discogs_releases.tsv")
    val DiscogsTracks = Value(s"$dataPath/data/discogs_tracks.tsv")
    val DiscogsTracksDeduplicated = Value(s"$dataPath/data/discogs_tracks_deduplicated")

    val ArtistTracksRelationship = Value(s"$dataPath/output/artist_track_relationships")
    val ArtistReleaseRelationship = Value(s"$dataPath/output/artist_release_relationships")
    val TracklistTrackRelationship = Value(s"$dataPath/output/tracklist_track_relationships")
    val RemixerTrackRelationship = Value(s"$dataPath/output/remixer_track_relationships")

    val ArtistNodes = Value(s"$dataPath/output/artist_nodes")
    val TracklistNodes = Value(s"$dataPath/output/tracklist_nodes")
    val TrackNodes = Value(s"$dataPath/output/track_nodes")

    val ArtistNodesTSV = Value(s"$dataPath/output/artist_nodes.tsv")
    val TracklistNodesTSV = Value(s"$dataPath/output/tracklist_nodes.tsv")
    val TrackNodesTSV = Value(s"$dataPath/output/track_nodes.tsv")
    val RelationshipsTSV = Value(s"$dataPath/output/relationships.tsv")

    def forNodes(nodeType:String): String = {
      Files.withName(s"$dataPath/output/${nodeType.toLowerCase}_nodes").toString
    }
  }
}
