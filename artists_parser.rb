require_relative 'xml_parser'

class ArtistsParser

  KNOWN_TAGS = %w(id name namevariations members aliases images #text realname data_quality profile urls groups)

  def self.parse(file, &block)
    Xml::Parser.new(file) do
      for_element 'artist' do
        current = {}
        inside_element do
          for_element 'id' do current[:id] = inner_xml end
          for_element 'name' do current[:name] = inner_xml end
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
end
