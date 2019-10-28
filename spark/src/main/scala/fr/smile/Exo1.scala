package fr.smile

import org.apache.spark.sql.{ DataFrame, SparkSession }

import org.slf4j.LoggerFactory

import org.apache.spark.sql.SaveMode

object Exo1  {

  val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    // Log info for debut
    logger.info("Launching application Exo1")

    // Load spark session
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    // Read DataFrame
    val pokemons = spark.read.json("src/main/resources/pokedex.json")

    // Order by spawn_chance
    val pokemonsOrdered = 
      pokemons
        .select("name", "spawn_chance")
        .orderBy("spawn_chance")

    // Write result
    pokemonsOrdered
      .repartition(3)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("results/exo1/")

  }

}
