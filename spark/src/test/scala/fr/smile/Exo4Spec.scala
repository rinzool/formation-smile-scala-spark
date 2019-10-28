package fr.smile 

import org.scalatest.{ FlatSpec, Matchers }

class Exo4Spec extends FlatSpec with Matchers with SparkSessionSupport {
  "pokemonsWithLiteralImc" should "add column with literal imc" in withSparkSession() {
    implicit spark => 
      
      import spark.implicits._

      val input = Seq(("id1", 16.9), ("id2", 21.0), ("id3", 28.5)).toDF("id", "imc")

      val result = Exo4.getLitImc(input)

      result.show

      result.where("id = 'id1'").collect.head.getAs[String]("lit_imc") shouldBe "< 18"
      result.where("id = 'id2'").collect.head.getAs[String]("lit_imc") shouldBe "18 - 25" 
      result.where("id = 'id3'").collect.head.getAs[String]("lit_imc") shouldBe "> 25" 
  }

  "joinWithImcReferential" should "join with imc" in withSparkSession() {
    implicit spark =>
    
      import spark.implicits._

      val input = Seq(("id1", "18 - 25")).toDF("id", "lit_imc")
      val imcReferentialDF = Exo4.imcReferential.toDF("lit_imc", "imc_reference")

      val result = Exo4.joinWithImcReferential(input, imcReferentialDF)

      result.show

      result.where("id = 'id1'").collect.head.getAs[String]("imc_reference") shouldBe "normal"
  }

  "countForEachImcCategory" should "group by and count" in withSparkSession() {
    implicit spark => 
      import spark.implicits._

      val input = Seq(("skinny", "name1"), ("skinny", "name2"), ("normal", "name3")).toDF("imc_reference", "name")

      val result = Exo4.countForEachImcCategory(input)

      result.show

      result.where("imc_reference = 'skinny'").collect.head.getAs[Int]("count") shouldBe 2
      result.where("imc_reference = 'normal'").collect.head.getAs[Int]("count") shouldBe 1
    }
}
