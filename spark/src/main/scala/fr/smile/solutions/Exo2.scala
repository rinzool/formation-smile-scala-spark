package fr.smile.solutions

import org.apache.spark.sql.{ DataFrame, SparkSession }

import org.slf4j.LoggerFactory

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

object Exo2  {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Log info for debut
    logger.info("Launching application Exo2")

    // Load spark session
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    // Read DataFrame
    val pokemons = spark.read.json("src/main/resources/pokedex.json")

    val pokemonsWithSquaredHeight = getSquaredHeight(pokemons)

    val pokemonsWithImc = calculateImc(pokemonsWithSquaredHeight)

    val pokemonsWithFinalColumns = keepNameAndImc(pokemonsWithImc)

    // Write result
    pokemonsWithFinalColumns 
      .repartition(3)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("results/exo2/")

  }


  /**
   * Create a new column "squared_height" with height * height value
   */
  def getSquaredHeight(pokemons: DataFrame): DataFrame = 
    pokemons.withColumn("squared_height", col("height") * col("height"))


  /**
   * Create a new column "imc" with value = with / (squared_height)
   */
  def calculateImc(pokemonsWithSquaredHeight: DataFrame): DataFrame = 
    pokemonsWithSquaredHeight.withColumn("imc", col("weight") / col("squared_height"))

  /**
   * Keep only columns "name" and "imc"
   */
  def keepNameAndImc(pokemonsWithImc: DataFrame): DataFrame = 
    pokemonsWithImc.select("name", "imc")


}
