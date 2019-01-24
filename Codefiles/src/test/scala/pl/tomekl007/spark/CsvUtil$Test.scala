package pl.tomekl007.spark

import org.scalatest.FunSuite
import pl.tomekl007.spark.algorithms.logistic_regression.LogisticRegressionWrapper.Statistic

import scala.collection.mutable

/**
  * Created by tomasz.lelek on 15/04/16.
  */
class CsvUtil$Test extends FunSuite {
  test("should write csv file") {

    CsvUtil.writeAsCsv(Map("tl" -> Statistic("tl", 0d, 0d, 1d, 1d, 1l, 0d, 1d)), "test")
  }

}
