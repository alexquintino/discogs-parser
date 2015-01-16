class CSVOutputter

  COL_SEPARATOR = "\t"

  def initialize(path)
    @path = path
    @buffer = []
    @file = File.open(@path, "w:UTF-8")
  end

  def write(fields)
    @file.write fields
        .map{ |field| field.is_a?(String) ? field : field.join(",")}
        .join(COL_SEPARATOR) + "\n"
  end

  def finalize
    @file.close
  end
end
