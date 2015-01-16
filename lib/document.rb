require "nokogiri"

class Document < Nokogiri::XML::SAX::Document

  def initialize(outputter = nil)
    @outputter = outputter
    @state = []
    @parsed_count = 0;
  end

  def parsed(fields)
    if non_empty?(fields)
      @outputter.write(fields)
      @parsed_count += 1
    end
  end

  def non_empty?(fields)
    fields.map { |field| !field.empty? }.reduce { |a,b| a && b }
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

  def skip_tag(tag)
    push :unknown
    @unknown_tag = tag
  end

  def skipping?
    peek == :unknown
  end

  def end_skip?(tag)
    if skipping?
      if tag == @unknown_tag
        pop
        @unknown_tag = nil
      end
      return true
    end
  end

end
