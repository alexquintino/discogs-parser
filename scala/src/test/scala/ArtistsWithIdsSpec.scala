
package test

import main.ArtistsWithIds
import org.scalatest._

class ArtistsWithIdsSpec extends FlatSpec with Matchers {

  it should "remove the number from the name and return the number" in {
    val name = "Something (6)"
    val (name_without_number, number) = ArtistsWithIds.remove_number(name)

    assert(number === "(6)")
    assert(name_without_number === "Something ")
  }

  it should "do nothing if there is no number" in {
    val name = "Something"
    val (name_without_number, number) = ArtistsWithIds.remove_number(name)

    assert(name_without_number === name)
  }

  it should "remove the number and switch first and last name" in {
    val artist = Array("123", "Persuader, The (6)")

    val fixed_artist = ArtistsWithIds.fixName(artist)
    assert(List("123", "The Persuader (6)") == fixed_artist)
  }

  it should "do nothing if there's no number of switch needed" in {
    val artist = Array("123", "Something")
    val fixed_artist = ArtistsWithIds.fixName(artist)

    assert(List("123", "Something") == fixed_artist)
  }

}
