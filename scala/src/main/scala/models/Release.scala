package models

import main.util.Normalizer

class Release(val id: Long, val master: String, val title: String, val artists: Array[Long]) extends Serializable with Node {
  def asNode: String = List(id, title, normalizedTitle, "Tracklist").mkString("\t")

  def normalizedTitle = Normalizer.normalize(title)
}
