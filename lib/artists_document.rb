require "cgi"
require "nokogiri"
require_relative "document"

class ArtistsDocument < Document

  def start_element(name, attrs = [])
    case name
    when "artist"
      push :artist
    when "name"
      push :name if peek == :artist
    when "id"
      push :id
    else
      push :unknown if peek == :artist
    end
  end

  def end_element(name, attrs = [])
    case name
    when "artist"
      parsed([@artist_id, @artist_name])
      @artist_id, @artist_name = nil
      @state.clear
    when "name"
      pop if peek == :name
    when "id"
      pop if peek == :id
    else
      pop if peek == :unknown
    end
  end

  def characters(string)
    if peek == :id
      @artist_id = string
    elsif peek == :name
      @artist_name = string
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

