package main.util

object Normalizer {
  def normalize(text: String): String = {
    java.text.Normalizer.normalize(text, java.text.Normalizer.Form.NFD)
      .toLowerCase
      .replaceAll("[^\\p{ASCII}]", "")
      .replaceAll("\t| ","")
  }
}
