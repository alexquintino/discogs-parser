class CSVOutputter

  COL_SEPARATOR = "\t"

  def initialize(path)
    @path = path
    @buffer = []
    @file = File.open(@path, "w")
  end

  def write(fields)
    @file.write fields.join(COL_SEPARATOR) + "\n"
  end

  def finalize
    @file.close
  end
end
