package pl.tomekl007.spark

import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.Matchers._

class VectorsTest extends SparkSuite {
  override def appName: String = "SVD-test"

  test("should create dense vector") {
    //given
    val data = Array(
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0))

    //then
    println(data.toList)
  }

  test("should create sparse vector") {
    //given
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))))

    //then
    println(data.toList)
  }



}
