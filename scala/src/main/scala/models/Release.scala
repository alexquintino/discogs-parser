package models

class Release(val id: String, val master: String, val title: String, val artistsIds: String) extends Serializable {

 val artists = artistsIds.split(",")
}
