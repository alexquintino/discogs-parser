require_relative '../lib/parser'
require_relative '../lib/masters_document'
require_relative '../lib/csv_outputter'
require "pry"

DATA = "data/discogs_20141001_masters.xml"
OUTPUT = "output/discogs_masters.tsv"
HEADERS = %w(discogs_master_id discogs_release_id artists)

trap :INT do
  Thread.main.raise Interrupt
end

Parser.parse(MastersDocument, DATA, CSVOutputter.new(OUTPUT))

