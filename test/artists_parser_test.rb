require_relative "../artists_parser"
require "minitest/autorun"

class ArtistsParserTest < MiniTest::Unit::TestCase

  def test_parse
    file = "test/test_xml.xml"
    result_id, result_name = nil
    ArtistsParser.parse(file) do |id, artist|
      result_id = id
      result_name = artist
    end
    assert_equal "3", result_id
    assert_equal "Josh Wink", result_name
  end

  def test_returns_the_name_correctly_if_theres_nothing_to_be_done
    assert_equal "Bla", ArtistsParser.new.fix_name("Bla")
  end

  def test_reverses_the_name_correctly
    assert_equal "Cenas Bla", ArtistsParser.new.fix_name("Bla, Cenas")
  end

  def test_moves_the_number_correctly
    assert_equal "Cenas Bla (6)", ArtistsParser.new.fix_name("Bla, Cenas (6)")
  end
end
