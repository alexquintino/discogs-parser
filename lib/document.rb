require "nokogiri"

class Document < Nokogiri::XML::SAX::Document

  def initialize(outputter = nil)
    @outputter = outputter
    @state = []
    @parsed_count = 0;
  end

  def parsed(fields)
    @outputter.write(fields)
    @parsed_count += 1
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

  def end_document
    @outputter.finalize
  end

  def how_many_parsed
    @parsed_count
  end
end
