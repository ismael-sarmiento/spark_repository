package components.tools

import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => spark_functions}


object SparkToolTransformer {

  def sparkTransformer(sparkSession: SparkSession): SparkToolTransformer = SparkToolTransformer(sparkSession)

}

case class SparkToolTransformer(sparkSession: SparkSession) {
  // para utilizar el formato heredado en una versión de spark
  sparkSession.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  def dateNormalization(column: Column, dateFormats: List[String]): Column = {
    spark_functions.coalesce(dateFormats.map(fmt => spark_functions.to_date(column, fmt)): _*)
  }

  def upsert(dfA: DataFrame, dfB: DataFrame) = ???
}
