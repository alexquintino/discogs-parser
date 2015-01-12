require "benchmark/ips"
require_relative "../artists_parser"
require "celluloid"

Benchmark.ips do |x|

  x.warmup = 2
  x.time = 20

  document = ArtistDocument.new
  text = "Persuader, The (6)"
  x.report("simple") { document.fix_name(text) }
  x.report("celluloid") { Celluloid::Future.new { document.fix_name(text) }}

end
