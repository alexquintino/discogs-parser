require "cgi"
require "nokogiri"

class ArtistsDocument < Nokogiri::XML::SAX::Document

  def initialize(block = nil)
    @block = block
    @state = []
    @parsed_count = 0
    @artist_id, @artist_name= nil
    @parsed_list = ""
    @parsed_list_size = 0
  end

  def start_element(name, attrs = [])
    case name
    when "artist"
      @state.push(:artist)
    when "name"
      @state.push(:name) if @state.last == :artist
    when "id"
      @state.push(:id)
    else
      @state.push(:unknown) if @state.last == :artist
    end
  end

  def end_element(name, attrs = [])
    case name
    when "artist"
      parsed(@block, @artist_id, @artist_name)
      @artist_id, @artist_name = nil
      @state.clear
    when "name"
      @state.pop if @state.last == :name
    when "id"
      @state.pop if @state.last == :id
    else
      @state.pop if @state.last == :unknown
    end
  end

  def characters(string)
    if @state.last == :id
      @artist_id = string
    elsif @state.last == :name
      @artist_name = fix_name(string)
    end
  end

  def end_document
    flush
  end

  def flush
    @block.call @parsed_list
    @parsed_list = ""
    @parsed_list_size = 0
  end

  def parsed(block, artist_id, artist_name)
    @parsed_list << "#{artist_id}\t#{artist_name}\n"
    @parsed_count += 1
    @parsed_list_size += 1
    if @parsed_list_size == 500000
      flush
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

  def how_many_parsed
    @parsed_count
  end
end

