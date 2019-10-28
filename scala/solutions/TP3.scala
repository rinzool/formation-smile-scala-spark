case class Person(name: String, birthyear: Int) {
  def getAge(): Int = 2019 - birthyear

  def displayNameAndAge(): String = s"$name is $getAge years old"
}

object TP3 {
  def main(args: Array[String]): Unit = {
    println("Bonjour")

    val people = Seq(
      Person("Jean", 1995),
      Person("Benoit", 1988),
      Person("Matt", 1983)
    )

    people.foreach(person => println(person.displayNameAndAge))

    val averageAge = people
      .map(person => person.getAge)
      .foldLeft(0)((x, y) => x + y / people.length)

    println(averageAge)
  }
}
