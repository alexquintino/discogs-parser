package models

class Release(val discogsId: String, val master: String, val title: String, val artists: Array[Long]) extends Serializable with Node {
  var id = discogsId

  def asNode: String = List(id, title, "Tracklist").mkString("\t")
}
