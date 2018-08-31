require "nokogiri"

class Document < Nokogiri::XML::SAX::Document

  OUTPUT_MAPPING = {first: 0, second: 1}

  def initialize(outputter = nil)
    @outputter = outputter
    @state = []
    @parsed_count = 0;
    @current_string = ""
  end

  def parsed(fields, allow_empty = false, outputter = :first)
    if allow_empty || non_empty?(fields)
      write(fields, outputter)
      @parsed_count += 1
    end
  end

  def write(array, outputter_index)
    if @outputter.is_a? Array
      @outputter[OUTPUT_MAPPING[outputter_index]].write(array)
    elsif outputter_index == :first
      @outputter.write(array)
    end
  end

  def end_document
    if @outputter.is_a? Array
      @outputter.each {|o| o.finalize}
    else
      @outputter.finalize
    end
  end

  def characters(string)
    if !skipping?
      @current_string += string
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

  def peek(index = 1)
    if index == 1
      @state.last
    else
      @state.last(index)
    end
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
