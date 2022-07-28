package app

import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val logFile = "README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName(getClass.getSimpleName).master("local").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"In README.md | Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
