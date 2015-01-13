require_relative '../lib/parser'
require_relative '../lib/artists_document'
require_relative '../lib/csv_outputter'
require 'csv'

DATA = "data/discogs_20141001_artists.xml"
OUTPUT = "output/discogs_artists.csv"
HEADERS = %w(discogs_id name)

trap :INT do
  Thread.main.raise Interrupt
end

Parser.parse(ArtistsDocument, DATA, CSVOutputter.new(OUTPUT))

