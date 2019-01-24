package pl.tomekl007.spark.algorithms.logistic_regression

import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.mllib.linalg._
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import pl.tomekl007.spark.SparkContextInitializer

class GMMWrapperTest extends FunSuite {
  test("Should predict and return probability of membership of each cluster") {
    //given
    val spark = SparkContextInitializer.createSparkContext("test", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        TestObj("a", DateTime.now().getMillis),
        TestObj("a", DateTime.now().plusHours(12).getMillis),
        TestObj("a", DateTime.now().plusHours(6).getMillis))
      )

    //when
    val gmm = GMMWrapper(data, "a")

    //then
    val vect = Vectors.dense(GMMWrapper.normalizeTimeAndGetHour(DateTime.now().getMillis))
    val res = gmm.predictSoft(vect).toList
    println(res)
    res.size shouldEqual 2
  }

  test("Should predict and return cluster number to which time-data is assigned when number of clusters is equal to 2") {
    //given
    val spark = SparkContextInitializer.createSparkContext("test", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        TestObj("a", DateTime.now().getMillis),
        TestObj("a", DateTime.now().plusHours(12).getMillis),
        TestObj("a", DateTime.now().plusHours(6).getMillis))
      )
    val numberOfClusters = 2
    //when
    val gmm = GMMWrapper(data, "a", numberOfClusters)

    //then
    val vect = Vectors.dense(GMMWrapper.normalizeTimeAndGetHour(DateTime.now().getMillis))
    val res = gmm.predict(vect)
    res == 1 || res == 0 shouldBe true
  }

  test("Should predict and return cluster number to which time-data is assigned when number of clusters is equal to 3") {
    //given
    val spark = SparkContextInitializer.createSparkContext("test", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        TestObj("a", DateTime.now().getMillis),
        TestObj("a", DateTime.now().plusHours(12).getMillis),
        TestObj("a", DateTime.now().plusHours(6).getMillis))
      )
    val numberOfClusters = 3
    //when
    val gmm = GMMWrapper(data, "a", numberOfClusters)

    //then
    val vect = Vectors.dense(GMMWrapper.normalizeTimeAndGetHour(DateTime.now().getMillis))
    val res = gmm.predict(vect)
    res == 2 || res == 1 || res == 0 shouldBe true
  }


  test("should normalize time for UTC") {
    //given
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).withHour(3)

    //when
    val res = GMMWrapper.normalizeTimeAndGetHour("ignore", timestamp.toInstant.getEpochSecond)

    //then
    res.equals(3)
  }

  test("should normalize time for UTC+1 in UTC timeZone") {
    //given
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC+1")).withHour(3)

    //when
    val res = GMMWrapper.normalizeTimeAndGetHour("ignore", timestamp.toInstant.getEpochSecond)

    //then
    res.equals(2)
  }


  case class TestObj(author: String, createdTimestamp: Long)

}
