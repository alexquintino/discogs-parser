require_relative "../artists_parser"

describe ArtistsParser do

  context "#fix_name" do

    it "returns the name correctly if there's nothing to be done" do
      expect(ArtistsParser.fix_name("Bla")).to eq("Bla")
    end
    it "reverses the name correctly" do
      expect(ArtistsParser.fix_name("Bla, Cenas")).to eq "Cenas Bla"
    end

    it "moves the number correctly" do
      expect(ArtistsParser.fix_name("Bla, Cenas (6)")).to eq("Cenas Bla (6)")
    end
  end
end
