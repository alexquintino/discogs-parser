require_relative 'xml_parser'
require "cgi"
require "pry"

class ArtistsParser

  KNOWN_TAGS = %w(id name namevariations members aliases images #text realname data_quality profile urls groups)

  def self.parse(file, &block)
    self.new.parse(file, &block)
  end

  def parse(file, &block)
    Xml::Parser.new(file) do
      for_element 'artist' do
        current = {}
        inside_element do
          for_element 'id' do current[:id] = inner_xml end
          for_element 'name' do current[:name] = ArtistsParser.fix_name(inner_xml) end
          inside_element 'namevariations' do end
          inside_element 'members' do end
          inside_element 'aliases' do end
          inside_element 'images' do end
          inside_element 'realname' do end
          inside_element 'data_quality' do end
          inside_element 'profile' do end
          inside_element 'urls' do end
          inside_element 'groups' do end
          raise "Don't know what to do with node=#{name} current=#{current}" unless KNOWN_TAGS.include?(name)
        end
        yield current
      end
    end
  end

  def self.fix_name(name)
    number = remove_number!(name)
    reversed = name.split(",").reverse.reduce("") { |mem,string| mem + " " + string.strip }.strip
    fixed_name = CGI.unescape_html(reversed)
    fixed_name << " #{number}" if number
    fixed_name
  end

  def self.remove_number!(name)
    number = name.scan(/\(\d+\)/).first
    name.gsub!(/\(\d+\)/, "") if number #remove number and add it to the end later
    number
  end
end

