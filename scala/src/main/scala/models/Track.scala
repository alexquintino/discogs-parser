package models

class Track(id0: String, val release: String, var artistsIds: String,  val title: String, val remixersIds: String) extends Serializable with Node {
  val artists = artistsIds.split(",")
  val remixers = remixersIds.split(",")
  var id = id0

  def allArtists: Array[String] = artists ++ remixers

  def asNode: String = List(id, title, "Track").mkString("\t")

}
