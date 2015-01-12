require_relative "../lib/artists_document"
require_relative "../lib/parser"
require "minitest/autorun"
require "pry"

class ArtistsParserTest < MiniTest::Unit::TestCase

  def test_parse
    file = "test/artists_sample.xml"
    result = []
    Parser.parse(ArtistsDocument, file) do |artist_string|
      result = artist_string.chomp.split("\t")
    end
    assert_equal "3", result[0]
    assert_equal "Josh Wink", result[1]
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
