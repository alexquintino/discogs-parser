package models

import main.util.Normalizer

class Artist(val id: Long, var name: String) extends Serializable with Node {
//  val id = discogsId
  name = fixName(name)

  def normalizedName: String = {
    Artist.normalize(name)
  }

  private
  def fixName(name: String): String = {
    val (name_without_number, number) = remove_number(name)
    val reversed = name_without_number.split(",").reverse.map(x => x.trim).mkString(" ")
    return if (number.isEmpty) reversed else s"$reversed $number"
  }

  // if there's a number, remove it and add it later
  def remove_number(name: String): (String, String) = {
    val pattern = """\(\d+\)""".r
    val number = pattern.findAllIn(name).toList.lastOption.getOrElse {
      return (name, "")
    }

    val name_without_number = pattern.replaceAllIn(name, "")
    return (name_without_number, number)
  }

  def asNode: String = List(id, name, Normalizer.normalize(name), "Artist").mkString("\t")


  def canEqual(other: Any): Boolean = other.isInstanceOf[Artist]

  override def equals(other: Any): Boolean = other match {
    case that: Artist =>
      (that canEqual this) &&
        id == that.id &&
        normalizedName == that.normalizedName
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, name)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Artist {

  def normalize(name: String): String = {
    Normalizer.normalize(name)
  }
}
