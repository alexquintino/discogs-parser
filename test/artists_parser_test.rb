require_relative "../lib/artists_document"
require_relative "../lib/parser"
require "minitest/autorun"
require "pry"

class ArtistsParserTest < MiniTest::Unit::TestCase

  def test_parse
    data = "test/artists_sample.xml"
    outputter = TestOutputter.new
    Parser.parse(ArtistsDocument, data, outputter)

    assert_equal "3", outputter.output.first.first[0]
    assert_equal "Josh Wink", outputter.output.first.first[1]
  end

  def test_returns_the_name_correctly_if_theres_nothing_to_be_done
    assert_equal "Bla", ArtistsDocument.new.fix_name("Bla")
  end

  def test_reverses_the_name_correctly
    assert_equal "Cenas Bla", ArtistsDocument.new.fix_name("Bla, Cenas")
  end

  def test_moves_the_number_correctly
    assert_equal "Cenas Bla (6)", ArtistsDocument.new.fix_name("Bla, Cenas (6)")
  end
end

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
