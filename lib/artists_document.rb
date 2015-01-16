require "cgi"
require "nokogiri"
require_relative "document"

class ArtistsDocument < Document

  IMPORTANT_TAGS = %w(artists artist id name)

  def initialize(*args)
    super
    @artist_name, @artist_id = "", ""
  end
  def start_element(name, attrs = [])
    return if skipping?
    if IMPORTANT_TAGS.include?(name)
      push name
    else
      skip_tag name
    end
  end

  def end_element(name, attrs = [])
    return if end_skip?(name)
    if name == "artist"
      parsed([@artist_id, @artist_name])
      @artist_id, @artist_name = "", ""
      @state.clear
    else
      pop
    end
  end

  def characters(string)
    if peek == "id"
      @artist_id = string
    elsif peek == "name"
      @artist_name += string
    end
  end

  # removing this temporarily
  # def fix_name(name)
  #   number = remove_number!(name)
  #   reversed = name.split(",").reverse!.reduce("") { |mem,string| mem + " " + string.strip }.strip
  #   fixed_name = CGI.unescape_html(reversed)
  #   fixed_name << " #{number}" if number
  #   fixed_name
  # end


  # def remove_number!(name)
  #   number = name.scan(/\(\d+\)/).last
  #   name.gsub!(/\(\d+\)/, "") if number #remove number and add it to the end later
  #   number
  # end
end

