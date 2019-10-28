package fr.smile

import org.apache.spark.sql.{ DataFrame, SparkSession }

import org.slf4j.LoggerFactory

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

object Exo4  {

  val logger = LoggerFactory.getLogger(getClass.getName)

  val imcReferential = Seq(("< 18", "skinny"), ("18 - 25", "normal"), ("> 25", "fat"))

  def main(args: Array[String]): Unit = {
    // Log info for debut
    logger.info("Launching application Exo4")

    // Load spark session
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._

    // Load imc referential 
    val imcReferentialDF = imcReferential.toDF("lit_imc", "imc_reference")

    // Read DataFrame
    val pokemons = spark.read.json("src/main/resources/pokedex.json")
    
    // *** EXO2 ***
    val pokemonsWithSquaredHeight = Exo2.getSquaredHeight(pokemons)

    val pokemonsWithImc = Exo2.calculateImc(pokemonsWithSquaredHeight)

    val pokemonsWithFinalColumns = Exo2.keepNameAndImc(pokemonsWithImc)
    // ***


    val pokemonsWithLiteralImc = getLitImc(pokemonsWithFinalColumns)

    val pokemonsWithImcReference = joinWithImcReferential(pokemonsWithLiteralImc, imcReferentialDF)

    val countImcReference = countForEachImcCategory(pokemonsWithImcReference)

    // Write result
    countImcReference
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("results/exo3/")

  }


  /**
   * Create a column "lit_imc" with a value :
   * - "< 18" if col(imc) < 18
   * - "> 25" if col(imc) > 25
   * - "18 - 25" otherwise 
   */
  def getLitImc(pokemons: DataFrame): DataFrame = 
    // TODO
    pokemons

  /**
   * Join with imcReferential on "lit_imc" column
   */
  def joinWithImcReferential(pokemons: DataFrame, imcReferentialDF: DataFrame): DataFrame =  
    // TODO
    pokemons

  /**
   * Group By imc_reference
   * Count 
   */
  def countForEachImcCategory(pokemons: DataFrame): DataFrame = 
    // TODO
    pokemons
    


}
