
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
    val name = "Persuader, The (6)"

    val fixed_name = ArtistsWithIds.fix_name(name)
    assert(fixed_name === "The Persuader (6)")
  }

  it should "do nothing if there's no number of switch needed" in {
    val name = "Something"
    val fixed_name = ArtistsWithIds.fix_name(name)

    assert(fixed_name === name)
  }

}
