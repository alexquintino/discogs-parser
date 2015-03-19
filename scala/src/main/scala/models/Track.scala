package models

import scala.util.hashing.MurmurHash3

class Track(id0: String, releasesIds: String, artistsIds: String,  val title: String, remixersIds: String) extends Serializable with Node {
  val artists = artistsIds.split(",")
  val remixers = remixersIds.split(",")
  val releases = releasesIds.split(",")
  var id = id0

  def allArtists: Array[String] = artists ++ remixers

  def asNode: String = List(id, title, "Track").mkString("\t")

  def hasRemixers: Boolean = !remixers.isEmpty

  def hash: Int = {
    val fields = allArtists.sorted(Ordering.String) ++ normalizedTitle
    MurmurHash3.arrayHash(fields)
  }

  def addReleases(otherReleases: Array[String]): Track = {
    new Track(id, (releases ++ otherReleases).mkString(","), artists.mkString(","), title, remixers.mkString(","))
  }

  override def toString: String = {
    val releasesStr = releases.mkString(",")
    val artistsStr = artists.mkString(",")
    val remixersStr = remixers.mkString(",")
    return s"$id\t$releasesStr\t$artistsStr\t$title\t$remixersStr"
  }

  private
  def normalizedTitle:String = title.toLowerCase()

}
