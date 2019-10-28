package fr.smile 

import org.scalatest.{ FlatSpec, Matchers }

class Exo2Spec extends FlatSpec with Matchers with SparkSessionSupport {
  "getSquaredHeight" should "calculate squared height" in withSparkSession() {
    implicit spark => 
      
      import spark.implicits._

      val input = Seq(("id1", 1.0), ("id2", 2.0)).toDF("id", "height")

      val result = Exo2.getSquaredHeight(input)

      result.show

      result.where("id = 'id1'").collect.head.getAs[Double]("squared_height") shouldBe 1.0
      result.where("id = 'id2'").collect.head.getAs[Double]("squared_height") shouldBe 4.0 
  }

  "calculateImc" should "calculate weight / height * height" in withSparkSession() {
    implicit spark =>
    
      import spark.implicits._

      val input = Seq(("id1", 1.0, 4.0)).toDF("id", "weight", "squared_height")

      val result = Exo2.calculateImc(input)

      result.show

      result.columns.contains("imc") shouldBe true 
      result.where("id = 'id1'").collect.head.getAs[Double]("imc") shouldBe 0.25
  }

  "keepNameAndImc" should "remove all unnecessary columns" in withSparkSession() {
    implicit spark => 
      import spark.implicits._

      val input = Seq(("a", "b", "c", "d", "e")).toDF("imc", "b", "c", "name", "e")

      val result = Exo2.keepNameAndImc(input)

      result.show

      result.columns.sorted shouldBe Array("imc", "name")
    }
}
