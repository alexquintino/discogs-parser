require_relative "../lib/releases_document"
require_relative "../lib/parser"
require_relative "test_outputter"
require "minitest/autorun"
require "pry"

class ReleasesDocumentTest < MiniTest::Test

  SAMPLE = "data/releases_sample.xml"

  def test_parse_release_id_main_artists_and_master
    outputter = TestOutputter.new
    Parser.parse(ReleasesDocument, SAMPLE, outputter)

    assert_equal "1", outputter.output[0][0]
    assert_equal "5427", outputter.output[0][1]
    assert_equal "Stockholm", outputter.output[0][2]
    assert_equal ["1"], outputter.output[0][3]

    assert_equal "3", outputter.output[2][0]
    assert_equal "66526", outputter.output[2][1]
    assert_equal "Profound Sounds Vol. 1", outputter.output[2][2]
    assert_equal ["3"], outputter.output[2][3]
  end

  def test_parse_track_output
    outputter = TestOutputter.new
    outputter2 = TestOutputter.new
    Parser.parse(ReleasesDocument, SAMPLE, [outputter,outputter2])

    #first 6 tracks belong to release 1
    assert outputter2.output[0..5].map{|t| t[0] == "1"}.reduce{|a,b| a && b}
    assert_equal "1", outputter2.output[0][0]
    assert_equal ["1"], outputter2.output[0][1]
    assert_equal "Östermalm", outputter2.output[0][2]

    # 5 tracks first release +  4 tracks second release.
    assert_equal "3", outputter2.output[10][0]
    assert_equal ["7"], outputter2.output[12][1]
    assert_equal "When The Funk Hits The Fan (Mood II Swing When The Dub Hits The Fan)", outputter2.output[12][2]
    assert_equal ["8"], outputter2.output[12][3]
  end

  def test_doesnt_pick_roles_other_than_remix
    outputter = TestOutputter.new
    outputter2 = TestOutputter.new
    Parser.parse(ReleasesDocument, SAMPLE, [outputter,outputter2])

    # song nr 10
    assert_equal [], outputter2.output[19][3]
  end
end
