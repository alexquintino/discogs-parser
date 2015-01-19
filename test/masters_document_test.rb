require_relative "../lib/masters_document"
require_relative "../lib/parser"
require_relative "test_outputter"
require "minitest/autorun"

class MastersDocumentTest < MiniTest::Test

  SAMPLE = "data/masters_sample.xml"

  def test_parse
    outputter = TestOutputter.new
    Parser.parse(MastersDocument, SAMPLE, outputter)

    assert_equal "18500", outputter.output[0][0]
    assert_equal "155102", outputter.output[0][1]
    assert_equal ["212070"], outputter.output[0][2]

    assert_equal "27794", outputter.output[2][0]
    assert_equal "1216600", outputter.output[2][1]
    assert_equal ["278","1409","199648"], outputter.output[2][2]
  end
end
