class TestOutputter

  attr_reader :output

  def initialize
    @output = []
  end

  def write(array)
    @output << array
  end

  def finalize
  end
end
