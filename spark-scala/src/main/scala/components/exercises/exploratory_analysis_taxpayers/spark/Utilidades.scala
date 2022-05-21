package components.exercises.exploratory_analysis_taxpayers.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utilidades {

  val filePath = "src/main/scala/components/exercises/exploratory_analysis_taxpayers/resources/adult.data.clean.csv"

  def loadDataset(file: String = filePath): DataFrame = {

    val dfContribuyentes: DataFrame = sparkSessionLocal.read.
      format("csv").
      option("header", "true").
      load(filePath)

    sparkSessionLocal.sparkContext.setLogLevel("warn")

    dfContribuyentes

  }

  def sparkSessionLocal: SparkSession = SparkSession.builder().master("local").getOrCreate()
}
