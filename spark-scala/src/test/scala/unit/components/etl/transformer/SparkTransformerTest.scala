package unit.components.etl.transformer

import components.etl.transformer.SparkTransformer
import mocks.SparkMock
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class SparkTransformerTest extends AnyFlatSpec with BeforeAndAfterAll with SparkMock {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // Init - Class
  override protected def beforeAll: Unit = {
    super.beforeAll()
    this.logger.info("Hola")
  }

  "A Dataframe with denormalized date column" should "execute the dateNormalization function return normalized column" in {
    val columns = Seq("id", "DateI", "DateII")
    val data = List(
      Row(0, "2021.03.08", null),
      Row(0, "2021.12.41", null),
      Row(1, null.asInstanceOf[String], null.asInstanceOf[String]),
      Row(2, "2020-12-10", "2020-01-10"),
      Row(2, "2020-R-10", "2020-01-10"),
      Row(2, "2020-01-99", "2020-01-10"),
      Row(2, "20-01-10", "2020-01-10"),
      Row(2, "2001-10", "2020-01-10"),
      Row(2, "20_01-10", "2020-01-10"),
      Row(2, "190001-10", "2020-01-10"),
      Row(2, "2020-01-10", "2020-01-10"),
      Row(2, "2020-12-31", "2020-01-10"),
      Row(2, "1904/01/26", "2020-01-10"),
      Row(2, "2021/12/26", "2020-01-10"),
      Row(2, "2020_01_30", "2020-01-10"),
      Row(2, "2020-Enero-10", "2020-01-10"),
      Row(3, "2019_Feb_07", null.asInstanceOf[String]),
      Row(4, "0200.1208", "0200.1308"),
      Row(5, "123", null.asInstanceOf[String])
    )
    val dateFormats = List("yyyy-MM-dd", "yyyy/MM/dd", "yyy.MM.dd", "yyyy.MMdd", "yyyy")
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = true),
      StructField("DateI", StringType, nullable = true),
      StructField("DateII", StringType, nullable = true)
    ))

    val dfOrigin = sparkSessionMock.createDataFrame(sparkSessionMock.sparkContext.parallelize(data), schema).toDF(columns: _*)

    val sparkTransformer = SparkTransformer(sparkSessionMock)
    val dfOrigin3 = dfOrigin.withColumn(
      "DateINormalization",
      sparkTransformer.dateNormalization(dfOrigin.col("DateI"), dateFormats)
    )

    assert(true)

  }

  // Finish - Class
  override protected def afterAll: Unit = {
    super.afterAll()
  }

}




//from pyspark.sql import *
//from pyspark.sql.functions import *
//from pyspark.sql.types import *
//
//
//
//def pyspark_transform(spark, df):
//  """
//  :param spark: SparkSession
//  :param df: Input DataFrame
//  :return: Transformed DataFrame
//  """
//  def to_date_multiple(column, formats=("MM/dd/yyyy", "yyyy-MM-dd", "dd-MM-yyyy")):
//  # Spark 2.2 or later syntax, for < 2.2 use unix_timestamp and cast
//  return coalesce(*[to_date(column, f) for f in formats])
//
//  df = df.withColumn("fechaNacimientoDate", to_date_multiple("fechaNacimiento"))
//
//  return df