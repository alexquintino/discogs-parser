require "pry"
require "csv"

DISCOGS_LIST = "output/discogs_artists.csv"
ARTISTS_LIST = ARGV[0]
OUTPUT_LIST = "output/artists_with_ids.csv"

def canonical(text)
  text.downcase.strip
end

def id_for(name)
  STATS[:count] += 1
  id = DISCOGS_ARTISTS[canonical(name)]
  if id.nil?
    STATS[:na_count] += 1
    STATS[:na] << name
  end
  id
end

DISCOGS_ARTISTS = {}
STATS = {count: 0, na_count: 0, na: []}

CSV.foreach(DISCOGS_LIST, :headers => :first_row, return_headers: false, col_sep: "\t") do |row|
  name = row['name']
  # puts "Repeated #{name}(#{row['discogs_id']}) with #{DISCOGS_ARTISTS[canonical(name)]}" if DISCOGS_ARTISTS.include?(canonical(name))
  DISCOGS_ARTISTS[canonical(name)] = row['discogs_id'] # if it exists, fuck it, it's going to be replaced with the last instance
end

CSV.open(OUTPUT_LIST, 'wb', {:headers => ["discogs_id", "name"], col_sep: "\t", write_headers: true, encoding: "UTF-8"}) do |csv|
  File.open(ARTISTS_LIST) do |file|
    file.each_line do |raw_name|
      name = raw_name.strip
      artist_id = id_for(name)
      csv << [artist_id, name] if artist_id
    end
  end
end

puts STATS
puts "Couldn't find #{(STATS[:na_count].to_f / STATS[:count].to_f) * 100}%"

