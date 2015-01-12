require "nokogiri"

class Document < Nokogiri::XML::SAX::Document

  def initialize(block = nil)
    @block = block
    @state = []
    @parsed_count = 0
    @parsed_list = ""
    @parsed_list_size = 0
  end

  def flush
    @block.call @parsed_list
    @parsed_list = ""
    @parsed_list_size = 0
  end

  def parsed(string)
    @parsed_list << string
    @parsed_count += 1
    @parsed_list_size += 1
    if @parsed_list_size == 500000
      flush
    end
  end

  def push(state)
    @state.push(state)
  end

  def pop
    @state.pop
  end

  def peek
    @state.last
  end
end
