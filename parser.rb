require "nokogiri"

class Parser
  def initialize(document, file)
    @time_start = Time.now
    @document = document
    @file = file
  end

  def self.parse(document, file, &block)
    self.new(document, file).parse(&block)
  end

  def parse(&block)
    begin
      document = @document.new(block)
      parser = Nokogiri::XML::SAX::Parser.new(document)
      parser.parse_file(@file)
    rescue Interrupt
      time_end = Time.now
      diff_time = time_end - @time_start
      puts "Speed=#{document.how_many_parsed / diff_time.to_f}"
    end
  end

end
