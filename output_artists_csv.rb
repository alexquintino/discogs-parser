require_relative 'artists_parser'
require 'csv'

FILE = "data/discogs_20141001_artists.xml"
CSV_FILE = "output/discogs_artists.csv"
CSV_HEADERS = %w(discogs_id name)

trap :INT do
  Thread.main.raise Interrupt
end

CSV.open(CSV_FILE, 'wb', {col_sep: "\t", encoding: "UTF-8"}) do |csv|
  csv << CSV_HEADERS
  ArtistsParser.parse(FILE) do |id, name|
    csv << [id, name]
  end
end
