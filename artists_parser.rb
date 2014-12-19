require_relative 'xml_parser'
require "cgi"

class ArtistsParser < Xml::Parser

  def self.parse(file, &block)
    self.new.parse(file, &block)
  end

  def initialize
    @time_start = Time.now
    @parsed = 0
    @artist_id, @artist_name= nil
    @parsed_list = ""
    @parsed_list_size = 0
  end

  def parse(file, &block)
    begin
      @node = Nokogiri::XML::Reader.from_io(File.new(file))
      @node.each do
        if is_start? && @node.name == "artist"
          handle_artist_node
          parsed(block)
        end
      end
      block.call @parsed_list
    rescue Interrupt
      time_end = Time.now
      diff_time = time_end - @time_start
      puts "Speed=#{@parsed / diff_time.to_f}"
    end
  end

  def handle_artist_node
    @node.each do
      if is_start? && @node.name == "name"
        @artist_name = fix_name(@node.inner_xml)
      elsif is_start? && @node.name == "id"
        @artist_id = @node.inner_xml
      elsif is_end? && @node.name == "artist"
        break
      else
        ignore_node
      end
    end
  end

  def parsed(block)
    @parsed_list << "#{@artist_id}\t#{@artist_name}\n"
    @artist_id, @artist_name = nil
    @parsed += 1
    @parsed_list_size += 1
    if @parsed_list_size == 500000
      block.call @parsed_list
      @parsed_list = ""
      @parsed_list_size = 0
    end
  end


  def fix_name(name)
    number = remove_number!(name)
    reversed = name.split(",").reverse!.reduce("") { |mem,string| mem + " " + string.strip }.strip
    fixed_name = CGI.unescape_html(reversed)
    fixed_name << " #{number}" if number
    fixed_name
  end

  def remove_number!(name)
    number = name.scan(/\(\d+\)/).last
    name.gsub!(/\(\d+\)/, "") if number #remove number and add it to the end later
    number
  end
end

