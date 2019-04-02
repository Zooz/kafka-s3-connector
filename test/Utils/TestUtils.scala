package Utils

object TestUtils {
  /**
   * Returns the content of the given resource file as string
   */
  def loadResourceFile(resourcePath: String): String = {
    val inputStream = Option(getClass.getResourceAsStream(resourcePath))
    if (inputStream.isEmpty) {
      throw new RuntimeException(s"Cannot find resource file needed for tests: $resourcePath")
    } else {
      scala.io.Source.fromInputStream(inputStream.get).mkString
    }
  }
}
