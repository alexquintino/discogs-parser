require "neography"
require "csv"

neo = Neography::Rest.new

result = neo.execute_query("MATCH (n:Artist)-->(t)<--(m:Artist) RETURN n.discogs_id,collect(DISTINCT m.discogs_id)")

CSV.open("output/artist_artist_relationship.tsv", "wb", {col_sep: "\t"}) do |csv|
  csv << ["Source", "Target"]
  result["data"].each do |rel|
    main_artist = rel[0]
    rel[1].each do |artist|
      csv << [main_artist, artist]
    end
  end
end
