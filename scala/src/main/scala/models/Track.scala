package models

import main.util.Normalizer
import scala.util.hashing.MurmurHash3

class Track(val id: Long, val releases: Array[Long], val artists: Array[Long],  val title: String, val remixers: Array[Long]) extends Serializable with Node {

  def allArtists: Array[Long] = (artists ++ remixers)

  def asNode: String = List(id, title, normalizedTitle, "Track").mkString("\t")

  def hasRemixers: Boolean = !remixers.isEmpty

  def hash: Int = {
    val fields = allArtists.sorted(Ordering.Long) ++ normalizedTitle
    MurmurHash3.arrayHash(fields)
  }

  def addReleases(otherReleases: Array[Long]): Track = {
    new Track(id, (releases ++ otherReleases), artists, title, remixers)
  }

  override def toString: String = {
    val releasesStr = releases.mkString(",")
    val artistsStr = artists.mkString(",")
    val remixersStr = remixers.mkString(",")
    return s"$id\t$releasesStr\t$artistsStr\t$title\t$remixersStr"
  }

  private
  def normalizedTitle = Normalizer.normalize(title)

}
