package models

class Release(val discogsId: String, val master: String, val title: String, val artistsIds: String) extends Serializable with Node {
  var id = discogsId
  val artists = artistsIds.split(",")

  def asNode: String = List(id, title, "Tracklist").mkString("\t")
}
