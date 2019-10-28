package fr.smile

import org.apache.spark.sql.SparkSession

trait SparkSessionSupport {

  def withSparkSession()(method: SparkSession => Unit): Unit = {
    implicit val spark: SparkSession =
      SparkSession.builder().master("local[*]").config("local.dir", "test_tmp").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      method(spark)
    } finally {
      spark.close
    }
  }
}
