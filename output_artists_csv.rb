require_relative 'artists_parser'
require 'pry'
require 'csv'

FILE = "data/discogs_20141001_artists.xml"
CSV_FILE = "output/artists.csv"
CSV_HEADERS = %w(discogs_id name)

CSV.open(CSV_FILE, 'wb', {headers: CSV_HEADERS, col_sep: "\t", write_headers: true, encoding: "UTF-8"}) do |csv|
  ArtistsParser.parse(FILE) do |artist|
    binding.pry
    csv << [artist[:id], artist[:name]]
  end
end
