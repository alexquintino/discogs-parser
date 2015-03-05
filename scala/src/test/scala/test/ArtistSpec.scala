package test

import models.Artist
import org.scalatest._

class ArtistSpec extends FunSpec with Matchers {

  it("removes switches first and last name with number at the end") {
    val artist = new Artist("123", "Persuader, The (6)")

    assert("The Persuader (6)" == artist.name)
  }

  it("does nothing if there's no number or switch needed") {
    val artist = new Artist("123", "Something")

    assert("Something" == artist.name)
  }
}
