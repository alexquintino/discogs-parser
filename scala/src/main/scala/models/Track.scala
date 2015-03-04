package models

class Track(val release: String, var artistsIds: String,  val title: String, val remixersIds: String) extends Serializable {
  val artists = artistsIds.split(",")
  val remixers = remixersIds.split(",")

  def allArtists: Array[String] = artists ++ remixers

}
