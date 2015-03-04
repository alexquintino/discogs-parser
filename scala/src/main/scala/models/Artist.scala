package models

class Artist(val discogsId: String, var name: String) extends Serializable{
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
}

object Artist {

  def normalize(name: String): String = {
    name.toLowerCase
  }
}
