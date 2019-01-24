package pl.tomekl007.spark.algorithms.logistic_regression

import org.scalatest.FunSuite
import org.scalatest.Matchers._

class LogisticRegressionWrapperTest extends FunSuite {
  test("should parse user input"){
    //given
    val res = LogisticRegressionWrapper.createUserInput("To jest tekst / 12")
    res._1 shouldEqual "To jest tekst "
    res._2.shouldEqual(12)
  }
}
