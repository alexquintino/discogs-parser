package main.util

import org.scalatest.{Matchers, FunSpec}

class NormalizerSpec extends FunSpec with Matchers {

  it ("Lowers the case") {
    assert(Normalizer.normalize("TEST") == "test")
  }
  it ("removes accents") {
    assert(Normalizer.normalize("Rüter") == "ruter")
  }

  it("removes spaces and tabs") {
    assert(Normalizer.normalize(" This is a\tTest ") == "thisisatest")
  }

  it("doesn't remove exclamation marks, parantheses and others") {
    assert(Normalizer.normalize("something!(blabla remix?)") == "something!(blablaremix?)")
  }
}
