package main

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

object FilterArtistsAndReleases {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("discogs-parser Filter Artists and Releases")
    val sc = new SparkContext(conf)
    val logger = Logger.getLogger("spark")

    // artist_id / name
    val artists_ids = sc.textFile("output/artists_with_ids")
                            .map(_.split("\t"))
                            .map(artist => artist(0))
                            .collect.toSet
    val artists_ids_broadcast = sc.broadcast(artists_ids)


    // release_id / artists / title / remixers - filter out empty tracks
    val tracks = sc.textFile("output/discogs_tracks.tsv").map(_.split("\t")).filter(_.size > 2).cache()

    // release ids taken from selected tracks - there will be repeated releases
    val releaseIdsFromTracks = grabTracksForArtists(tracks, artists_ids_broadcast.value).map(track => track(0)).distinct.collect.toSet


    val releaseIdsFromTracksBroadcast = sc.broadcast(releaseIdsFromTracks)

    // release_id / master_id / title / main_artists - filter out malformed releases
    val releases = sc.textFile("output/discogs_releases.tsv").map(_.split("\t")).filter(_.size == 4).cache()
    val selected_releases = releases.filter(release => releaseIdsFromTracksBroadcast.value.contains(release(0)))

    // master ids taken from releases
    val masterIdsFromReleases = selected_releases.map(release => release(1)).distinct.collect.toSet



    // master_id / main_release / artists
    val masters = sc.textFile("output/discogs_masters.tsv").map(_.split("\t"))
    val selected_masters  = masters.filter(master => masterIdsFromReleases.contains(master(0)))

    // main releases extracted from master ids
    val mainReleasesFromMasters = selected_masters.map(master => master(1)).collect.toSet



    // from the selected releases before, filter the ones that are not the main release
    val finalReleaseList = selected_releases.filter(release => isMainRelease(release, mainReleasesFromMasters))
    val finalReleaseListIds = finalReleaseList.map(release => release(0)).collect.toSet
    val bfinalReleaseListIds = sc.broadcast(finalReleaseListIds)

    val finalTrackList = tracks.filter(track => bfinalReleaseListIds.value.contains(track(0)))

    finalReleaseList.coalesce(8).map(_.mkString("\t")).saveAsTextFile("output/releases")
    finalTrackList.coalesce(8).map(_.mkString("\t")).saveAsTextFile("output/tracks")
  }

  def grabTracksForArtists(tracks: RDD[Array[String]], artists_ids: Set[String]): RDD[Array[String]] = {
    tracks.filter(track => containsArtists(trackArtists(track), artists_ids))
  }

  def containsArtists(artists: Array[String], artists_ids: Set[String]): Boolean = {
    artists.map(id => artists_ids.contains(id)).fold(false)((bool, res) => bool || res)
  }

  def trackArtists(track: Array[String]): Array[String] = {
    if(track.size >= 3) {
      if(track.size == 3) {
        return track(1).split(",")
      }
      else
      {
        return track(1).split(",") ++ track(3).split(",")
      }
    } else {
      return Array()
    }
  }

  def isMainRelease(release: Array[String],  mainReleasesFromMasters: Set[String]): Boolean = {
    // doesn't have a master
    if(release(1).isEmpty)
    {
      return true
    }
    else
    {
      return mainReleasesFromMasters.contains(release(0))
    }
  }

}
