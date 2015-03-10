package models

import scala.util.hashing.MurmurHash3

class Track(id0: String, release: String, var artistsIds: String,  val title: String, val remixersIds: String) extends Serializable with Node {
  val artists = artistsIds.split(",")
  val remixers = remixersIds.split(",")
  val releases = Array(release)
  var id = id0

  def allArtists: Array[String] = artists ++ remixers

  def asNode: String = List(id, title, "Track").mkString("\t")

  def hasRemixers: Boolean = !remixers.isEmpty

  override def hashCode: Int = {
    val fields = allArtists.sorted(Ordering.String) ++ normalizedTitle
    MurmurHash3.arrayHash(fields)
  }

  def addReleases(otherReleases: Array[String]): Track = {
    releases ++ otherReleases
    this
  }

  private
  def normalizedTitle:String = title.toLowerCase()

}
