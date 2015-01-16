package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.FileWriter

object ArtistsWithIds {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser Fav Artists With Ids")
    val sc = new SparkContext(conf)

    val discogs_artists = sc.textFile(args(0))
    val fav_artists = scala.io.Source.fromFile(args(1)).getLines.toList.map(artist => normalize(artist))

    val found_artists = discogs_artists.map(line => line.split("\t"))
                  .map(array => Array(array(0), fix_name(array(1))))
                  .filter(array => fav_artists.contains(normalize(array(1))) )
                  .collect

    save_to_tsv(found_artists, args(2))

    print_not_found_stats(fav_artists, found_artists)

  }

  def save_to_tsv(array: Array[Array[String]], path: String) {
    val file = new FileWriter(path)
    array.foreach(element => file.write(element.mkString("\t") + "\n"))
    file.close
  }

  def print_not_found_stats(fav_artists: List[String], found_artists: Array[Array[String]]) {
    val found_artists_names = found_artists.map(fields => normalize(fields(1)))
    val not_found_artists = fav_artists.filter(artist => !found_artists_names.contains(artist))
    val percentage = not_found_artists.size.toDouble / fav_artists.size  * 100
    println(s"Not found: ${not_found_artists.size}. Percentage: $percentage")
    println(not_found_artists)
  }

  def normalize(string: String): String = {
    string.toLowerCase
  }

  def fix_name(name: String): String = {
    val (name_without_number, number) = remove_number(name)
    val reversed = name_without_number.split(",").reverse.map(x => x.trim).mkString(" ")
    if (number.isEmpty)
    {
      return reversed
    }
    else
    {
      return s"$reversed $number"
    }
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
