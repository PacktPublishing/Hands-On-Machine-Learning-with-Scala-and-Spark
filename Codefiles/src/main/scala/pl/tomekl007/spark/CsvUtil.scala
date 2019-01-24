package pl.tomekl007.spark

import org.joda.time.DateTime
import pl.tomekl007.spark.algorithms.logistic_regression.LogisticRegressionWrapper.Statistic

/**
  * Created by tomasz.lelek on 15/04/16.
  */
object CsvUtil {

  def writeAsCsv(result: Map[String, Statistic], testCasePrefix: String) = {
    val fileName = s"logistic_regression_$testCasePrefix _${DateTime.now().getMillis}.csv"
    val header = "author,precision,recall,falsePositiveRate,truePositiveRate,numberOfPosts,areaUnderROC,threshold\n"
    val content = result.map(line => s"${line._2.author},${line._2.precision}" +
      s",${line._2.recall},${line._2.falsePositiveRate},${line._2.truePositiveRate},${line._2.numberOfPosts}" +
      s",${line._2.areaUnderROC},${line._2.threshold}\n").reduce((a, b) => a + b)

    scala.tools.nsc.io.File(fileName).writeAll(header + content)

  }
}
