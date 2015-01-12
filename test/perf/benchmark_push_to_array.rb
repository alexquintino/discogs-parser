require "benchmark/ips"

Benchmark.ips do |x|

  x.warmup = 2
  x.time = 20

  def merge(state1, state2)
    "#{state1}_#{state2}"
  end

  STATES = ["artist", "name", "id"]
  ARRAY = []

  x.report("simple strings") do
    ARRAY.push(STATES[0])
    ARRAY.push(merge(STATES[0],STATES[1]))
    ARRAY.push(merge(STATES[1],STATES[2]))
    ARRAY.pop
    ARRAY.pop
    ARRAY.pop
  end

  x.report("symbols") do
    ARRAY.push(STATES[0].to_sym)
    ARRAY.push(merge(STATES[0],STATES[1]).to_sym)
    ARRAY.push(merge(STATES[1],STATES[2]).to_sym)
    ARRAY.pop
    ARRAY.pop
    ARRAY.pop
  end

  x.report("arrays") do
    ARRAY.push([STATES[0]])
    ARRAY.push([STATES[0], STATES[1]])
    ARRAY.push([STATES[1], STATES[2]])
    ARRAY.pop
    ARRAY.pop
    ARRAY.pop
  end

end
