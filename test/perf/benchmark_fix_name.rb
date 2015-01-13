require "benchmark/ips"
require_relative "../../lib/artists_document"
require "celluloid"

Benchmark.ips do |x|

  x.warmup = 2
  x.time = 20

  document = ArtistsDocument.new
  text = "Persuader, The (6)"
  x.report("simple") { document.fix_name(text) }
  x.report("celluloid") { Celluloid::Future.new { document.fix_name(text) }}

end
