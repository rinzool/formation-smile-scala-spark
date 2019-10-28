package fr.smile 

import org.scalatest.{ FlatSpec, Matchers }

class Exo3Spec extends FlatSpec with Matchers with SparkSessionSupport {

  "groupByChannel" should "group and count videos by channel" in withSparkSession() {
    implicit spark =>

      import spark.implicits._
      
      val input = Seq(("cyprien", "hello"), ("cyprien", "bye"), ("misterv", "lol")).toDF("channel_title", "video_title")

      val result = Exo3.groupByChannel(input)

      result.show

      result.where("channel_title = 'cyprien'").collect.head.getAs[Int]("total_videos") shouldBe 2
      result.where("channel_title = 'misterv'").collect.head.getAs[Int]("total_videos") shouldBe 1
      result.count shouldBe 2
  }

  "sortVideos" should "sort videos descendant" in withSparkSession() {
    implicit spark => 

      import spark.implicits._

      
      val input = Seq(("cyprien", 2), ("misterv", 1), ("mcfly&carlito", 42)).toDF("channel_title", "total_videos")

      val result = Exo3.sortVideos(input)

      result.show

      result.collect.head.getAs[String]("channel_title") shouldBe "mcfly&carlito"


  }
}
