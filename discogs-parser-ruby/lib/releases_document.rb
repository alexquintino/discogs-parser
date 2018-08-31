require_relative 'document'
require "pry"

class ReleasesDocument < Document

 IMPORTANT_TAGS =
    %w(releases release artists artist tracklist track title extraartists id role master_id)

  def initialize(*args)
    super
    init_release
  end

  def start_element(name, attrs = [])
    return if skipping?
    if IMPORTANT_TAGS.include?(name)
      if name == 'release'
        @release_data[:id] = attrs.select{ |attr| attr[0] == 'id' }.first[1]
      end
      push name
      @current_string = ""
    else
      skip_tag name
    end
  end

  def end_element(name, attrs = [])
    return if end_skip?(name)
    case name
    when "release"
      parsed(@release_data)
      @state.clear
      init_release
    when 'id'
      case peek(4)
      when ["release","artists", "artist", "id"]
        @release_data[:main_artists] << @current_string
      when ["track", "artists", "artist", "id"]
        @track[:artists] << @current_string
      when ["track", "extraartists", "artist", "id"]
        @artist_role[:id] = @current_string
      end
    when 'master_id'
      @release_data[:master_id] = @current_string
    when 'track'
      @release_data[:tracks] << @track
      init_track
    when 'title'
      if peek(2) == ['release', 'title']
        @release_data[:title] = @current_string
      elsif peek(2) == ['track', 'title']
        @track[:title] = @current_string
      end
    when 'role'
      if peek(4) == ["track", "extraartists", "artist", "role"]
        if @current_string == "Remix"
          @artist_role[:role] = "Remix"
        end
      end
    when 'artist'
      if peek(3) == ["track", "extraartists", "artist"]
        if @artist_role[:role] == "Remix"
          @track[:remixed] << @artist_role[:id]
        end
        @artist_role = {}
      end
    end
    pop
    @current_string = ""
  end

  def parsed(release)
    super [release[:id], release[:master_id], release[:title], release[:main_artists]], true
    release[:tracks].each do |track|
      artists = track[:artists].empty? ? release[:main_artists] : track[:artists]
      super [release[:id], artists, track[:title], track[:remixed]], true, :second
    end
  end

  def init_release
    @release_data = {main_artists: [], tracks: [], master_id: ""}
    @artist_role = {}
    init_track
  end

  def init_track
    @track = {artists: [], remixed: []}
  end



end
