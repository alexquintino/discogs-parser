require_relative 'parser'
require_relative 'artists_document'
require 'csv'

FILE = "data/discogs_20141001_artists.xml"
CSV_FILE = "output/discogs_artists.csv"
CSV_HEADERS = %w(discogs_id name)

trap :INT do
  Thread.main.raise Interrupt
end

File.open(CSV_FILE, 'wb') do |csv|
  csv.puts CSV_HEADERS.join("\t")
  Parser.parse(ArtistsDocument, FILE) do |artists|
    csv.write(artists)
  end
end
