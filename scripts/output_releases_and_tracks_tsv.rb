require_relative '../lib/parser'
require_relative '../lib/releases_document'
require_relative '../lib/csv_outputter'
require "pry"

DATA = "data/discogs_20141001_releases.xml"
OUTPUT = "output/discogs_releases.tsv"
OUTPUT2 = "output/discogs_tracks.tsv"
HEADERS = %w(discogs_release_id discogs_masters_id title main_artists)
HEADERS2 = %w(discogs_release_id discogs_artists_id title remixed_artists)

trap :INT do
  Thread.main.raise Interrupt
end

Parser.parse(ReleasesDocument, DATA, [CSVOutputter.new(OUTPUT), CSVOutputter.new(OUTPUT2)])

