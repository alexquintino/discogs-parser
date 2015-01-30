require "tempfile"

HEADERS_ARTISTS = %w(i:id discogs_id:int name:string l:label)
HEADERS_TRACKLISTS = %w(i:id discogs_id:int title:string l:label)
HEADERS_TRACKS = %w(i:id title:string l:label)
HEADERS_RELATIONSHIPS = %w(start end type)

def output_artists_tsv
  headers = Tempfile.new("artists_headers")
  headers.write HEADERS_ARTISTS.join("\t") + "\n"
  headers.close
  %x( cat #{headers.path} output/artists_nodes/part-* > output/artists_nodes.tsv )
  headers.unlink
end

def output_tracklists_tsv
  headers = Tempfile.new("tracklists_headers")
  headers.write HEADERS_TRACKLISTS.join("\t") + "\n"
  headers.close
  %x( cat #{headers.path} output/tracklists_nodes/part-* > output/tracklists_nodes.tsv )
  headers.unlink
end

def output_tracks_tsv
  headers = Tempfile.new("tracks_headers")
  headers.write HEADERS_TRACKS.join("\t") + "\n"
  headers.close
  %x( cat #{headers.path} output/tracks_nodes/part-* > output/tracks_nodes.tsv )
  headers.unlink
end

def output_relationships_tsv
  headers = Tempfile.new("relationships_headers")
  headers.write HEADERS_RELATIONSHIPS.join("\t") + "\n"
  headers.close
  %x( cat #{headers.path} output/artist_release_relationships/part-* output/tracklist_track_relationships/part-* output/artist_track_relationships/part-* > output/relationships.tsv )
  headers.unlink
end

output_artists_tsv
output_tracklists_tsv
output_tracks_tsv
output_relationships_tsv
