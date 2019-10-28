package fr.smile

import org.apache.spark.sql.{ DataFrame, SparkSession }

import org.slf4j.LoggerFactory

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

object Exo3  {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Log info for debut
    logger.info("Launching application Exo3")

    // Load spark session
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Read DataFrame
    val videos = spark.read.option("header", true).csv("src/main/resources/youtube.csv")

    val videosGroupedByChannel = groupByChannel(videos)

    val sortedVideos = sortVideos(videosGroupedByChannel)

    // Write result
    sortedVideos 
      .write
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save("results/exo4/")
  }


  /**
   * Group By "channel_title" and count for each channel in a column "total_videos"
   */
  def groupByChannel(videos: DataFrame): DataFrame = 
    // TODO
    videos

  /**
   * Sort videos by "total_videos" in a descendant way 
   */
  def sortVideos(videos: DataFrame): DataFrame = 
    // TODO
    videos


}
