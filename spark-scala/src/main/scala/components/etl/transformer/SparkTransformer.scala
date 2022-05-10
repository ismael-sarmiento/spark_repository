package components.etl.transformer

import org.apache.spark.sql.{Column, SparkSession, functions => spark_functions}


object SparkTransformer {

  def sparkTransformer(sparkSession: SparkSession): SparkTransformer = SparkTransformer(sparkSession)

}

case class SparkTransformer(sparkSession: SparkSession) {
  // para utilizar el formato heredado en una versión de spark
  sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  def dateNormalization(column: Column, dateFormats: List[String]): Column = {
    spark_functions.coalesce(dateFormats.map(fmt => spark_functions.to_date(column, fmt)): _*)
  }
}
