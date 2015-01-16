require_relative "document"

class MastersDocument < Document

  IMPORTANT_TAGS = %w(masters master main_release artists artist id)

  def initialize(*args)
    super
    init_fields
  end

  def start_element(name, attrs = [])
    return if skipping?
    if IMPORTANT_TAGS.include?(name)
      if name == "master"
        @masters_id = attrs.select{ |attr| attr[0] == "id" }.first[1]
      end
      push name
    else
      skip_tag name
    end
  end

  def end_element(name, attrs = [])
    return if end_skip?(name)
    if name == "master"
      parsed([@masters_id, @release_id, @artists])
      init_fields
      @state.clear
    elsif name == "id"
      @artists << @id
      @id = ""
      pop
    else
      pop
    end
  end

  def characters(string)
    if peek == "main_release"
      @release_id += string
    elsif peek == "id"
      @id += string
    end
  end

  def init_fields
    @masters_id, @release_id = "", ""
    @id = ""
    @artists = []
  end
end
