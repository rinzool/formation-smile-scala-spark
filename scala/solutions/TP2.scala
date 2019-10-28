case class Person(name: String, birthyear: Int) {
  def getAge(): Int = 2019 - birthyear

  def displayNameAndAge(): String = s"$name is $getAge years old"
}

object TP2 {
  def main(args: Array[String]): Unit = {
    println("Bonjour")

    val name = args(0)
    val date = args(1).toInt

    val person = Person(name, date)

    println(person.displayNameAndAge)
  }
}
