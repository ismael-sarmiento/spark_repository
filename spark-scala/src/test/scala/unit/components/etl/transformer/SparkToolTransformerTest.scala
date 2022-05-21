package unit.components.etl.transformer

import components.tools.SparkToolTransformer
import mocks.SparkMock
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.{Logger, LoggerFactory}

class SparkToolTransformerTest extends AnyFlatSpec with BeforeAndAfterAll with SparkMock {

  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  // Init - Class
  override protected def beforeAll: Unit = {
    super.beforeAll()
  }

  "A Dataframe with denormalized date column" should "execute dateNormalization function return normalized column" in {
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

    val sparkTransformer = SparkToolTransformer(sparkSessionMock)
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

  "Two Dataframe with columns" should "execute upsert function return a UpsertDataframe" in {
    val matches = sparkSessionMock.sparkContext.parallelize(List(Row(1, "John Wayne", "John Doe"), Row(2, "Ive Fish", "San Simon")))

    val players = sparkSessionMock.sparkContext.parallelize(Seq(
      Row("John Wayne", 1986),
      Row("Ive Fish", 1990),
      Row("San Simon", 1974),
      Row("John Doe", 1995)
    ))

    val matchesDf: DataFrame = sparkSessionMock.createDataFrame(
      matches, StructType(Seq(
        StructField("matchId", IntegerType, nullable = false),
        StructField("player1", StringType, nullable = false),
        StructField("player2", StringType, nullable = false)))
    ).as(Symbol("matches"))

    val playersDf: DataFrame = sparkSessionMock.createDataFrame(players, StructType(Seq(
      StructField("player", StringType, nullable = false),
      StructField("birthYear", IntegerType, nullable = false)
    ))).as(Symbol("players"))

    // see: https://sparkbyexamples.com/spark/spark-sql-dataframe-join/
    // see: https://towardsdatascience.com/strategies-of-spark-join-c0e7b4572bcf
    // insert
    val insertDF = matchesDf.join(playersDf, col("player1") === col("player"), "right").filter(!col("player1").isNull)
    // mejor insert
    playersDf.join(matchesDf, col("player1") === col("player"), "left")

      //      .select($"matches.matchId" as "matchId", $"matches.player1" as "player1", $"matches.player2" as "player2", $"players.birthYear" as "player1BirthYear")
      //      .join(playersDf, $"player2" === $"players.player")
      //      .select($"matchId" as "MatchID", $"player1" as "Player1", $"player2" as "Player2", $"player1BirthYear" as "BYear_P1", $"players.birthYear" as "BYear_P2")
      //      .withColumn("Diff", abs(Symbol("BYear_P2").(Symbol("BYear_P1"))))
      .show()

  }

}

