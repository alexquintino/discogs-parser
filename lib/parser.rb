require "nokogiri"
require_relative "csv_outputter"

class Parser

  def initialize(document, data, outputter)
    @time_start = Time.now
    @document = document
    @data = data
    @outputter = outputter
  end

  def self.parse(document, data, outputter)
    self.new(document, data, outputter).parse
  end

  def parse
    begin
      document = @document.new(@outputter)
      parser = Nokogiri::XML::SAX::Parser.new(document)
      parser.parse_file(@data)
    rescue Interrupt
      time_end = Time.now
      diff_time = time_end - @time_start
      puts "Speed=#{document.how_many_parsed / diff_time.to_f}"
    end
  end

end
