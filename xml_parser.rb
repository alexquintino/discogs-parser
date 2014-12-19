require 'nokogiri'

module Xml
  class Parser

    def is_start?
      @node.node_type == Nokogiri::XML::Reader::TYPE_ELEMENT
    end

    def is_end?
      @node.node_type == Nokogiri::XML::Reader::TYPE_END_ELEMENT
    end

    def ignore_node
      return if @node.name == "#text" || is_end?
      node_name = @node.name
      while(@node.read) do
        break if @node.name == node_name && is_end?
      end
  end

  end
end
