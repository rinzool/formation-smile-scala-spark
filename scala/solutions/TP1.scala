object TP1 {
  def main(args: Array[String]): Unit = {
    println("Hi")

    val name = args(0)
    val date = args(1).toInt

    val ageOf = getAgeOf(name, date)

    println(ageOf)
  }

  def getAgeOf(name: String, birthyear: Int): String =
    s"$name is ${2019 - birthyear} years old"
}
