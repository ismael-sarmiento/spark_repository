package mocks

import org.apache.spark.sql.SparkSession

trait SparkMock {

  def sparkSessionMock: SparkSession = SparkSession.builder().master("local").getOrCreate()

}
