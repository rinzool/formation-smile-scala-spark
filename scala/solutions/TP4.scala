sealed trait Sport
case class Natation() extends Sport
case class Athletism() extends Sport
case class Climbing() extends Sport

case class Person(name: String, birthyear: Int, sport: Sport) {
  def getAge(): Int = 2019 - birthyear

  def displayNameAndAge(): String = {
    val doingSport = sport match {
      case _: Natation => "swimming"
      case _: Athletism => "running"
      case _ => "doing sport"
    }
    s"$name is $getAge years old and likes $doingSport"
  }
}

object TP3 {
  def main(args: Array[String]): Unit = {
    println("Bonjour")

    val people = Seq(
      Person("Jean", 1995, Athletism()),
      Person("Benoit", 1988, Climbing()),
      Person("Matt", 1983, Natation())
    )

    people.foreach(person => println(person.displayNameAndAge))

    val averageAge = people
      .map(person => person.getAge)
      .foldLeft(0)((x, y) => x + y / people.length)

    println(averageAge)
  }
}
