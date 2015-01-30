package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.FileWriter
import org.apache.spark.rdd.RDD

object ArtistsWithIds {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser Fav Artists With Ids")
    val sc = new SparkContext(conf)

    // (id, name)
    val discogs_artists = sc.textFile("output/discogs_artists.csv").cache()
        .map(_.split("\t"))
        .map(fixName)
        .map(artist => (normalize(artist(1)), artist)) // (name_norm, (id, name))

    val fav_artists = sc.textFile(args(0))
        .map(artist => (normalize(artist), artist)) // (name_norm, name)

    // (name_norm, (name, (id, name))
    val found_artists = fav_artists.join(discogs_artists)

    found_artists.map(map => map._2._2).coalesce(1).distinct.map(_.mkString("\t")).saveAsTextFile("output/artists_with_ids")

    print_not_found_stats(fav_artists, found_artists)

  }

  def print_not_found_stats(fav_artists: RDD[(String,String)], found_artists: RDD[(String,(String, List[String]))]) {
    val not_found_artists = fav_artists.subtractByKey(found_artists).collect
    val size = not_found_artists.size.toDouble
    val percentage = size.toDouble / fav_artists.count  * 100
    println(s"Not found: ${size}. Percentage: $percentage")
    not_found_artists.foreach(artist => println(artist._2))
  }

  def normalize(string: String): String = {
    string.toLowerCase
  }

  def fixName(artist: Array[String]): List[String] = {
    val name = artist(1)
    val (name_without_number, number) = remove_number(name)
    val reversed = name_without_number.split(",").reverse.map(x => x.trim).mkString(" ")
    if (number.isEmpty)
    {
      return List(artist(0), reversed)
    }
    else
    {
      return List(artist(0), s"$reversed $number")
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
