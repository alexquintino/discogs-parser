# encoding: UTF-8

require_relative "../lib/artists_document"
require_relative "../lib/parser"
require_relative "test_outputter"
require "minitest/autorun"
require "pry"

class ArtistsParserTest < MiniTest::Test

  def test_parse
    data = "test/artists_sample.xml"
    outputter = TestOutputter.new
    Parser.parse(ArtistsDocument, data, outputter)

    assert_equal "3", outputter.output[0][0]
    assert_equal "Josh Wink", outputter.output[0][1]

    assert_equal "239", outputter.output[1][0]
    assert_equal "Jesper Dahlbäck", outputter.output[1][1]

    assert_equal "22164", outputter.output[3][0]
    assert_equal "Earth, Wind & Fire", outputter.output[3][1]
  end

  # def test_returns_the_name_correctly_if_theres_nothing_to_be_done
  #   assert_equal "Bla", ArtistsDocument.new.fix_name("Bla")
  # end

  # def test_reverses_the_name_correctly
  #   assert_equal "Cenas Bla", ArtistsDocument.new.fix_name("Bla, Cenas")
  # end

  # def test_moves_the_number_correctly
  #   assert_equal "Cenas Bla (6)", ArtistsDocument.new.fix_name("Bla, Cenas (6)")
  # end

  def test_non_empty?
    assert ArtistsDocument.new.non_empty?(["aaa", "bbb", "ccc"])
    refute ArtistsDocument.new.non_empty?(["aaa", "", "ccc"])
  end
end


