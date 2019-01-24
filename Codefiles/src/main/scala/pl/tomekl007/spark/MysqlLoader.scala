package pl.tomekl007.spark

import org.apache.spark.sql.{DataFrame, SQLContext}
import pl.tomekl007.spark.algorithms.logistic_regression.LogisticRegressionWrapper
import pl.tomekl007.spark.algorithms.{Word2VecImpl, Word2VectWithModel}
import pl.tomekl007.spark.model.Message
import pl.tomekl007.spark.utils.InputCleanUtil

import scala.collection.mutable.ArrayBuffer


//mysql.server start
//mysql -u root
class MysqlLoader(sqlContext: SQLContext, runConfig: RunConfig) {

  def load(): Unit = {

    val dataframe_mysql =
      sqlContext.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost/forum")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "b_messages")
        .option("user", "root")
        .option("password", "").load()

    val cleanedDf = InputCleanUtil.tokenizeAndStopWords(dataframe_mysql, sqlContext)
    val topAuthors = getTopAuthorsFromNumberOfPosts(cleanedDf, 1)


    val vectorized = Word2VecImpl(topAuthors, runConfig)

    LogisticRegressionWrapper(vectorized.dataFrame, sqlContext.sparkContext, runConfig)
  }


  def findSynonyms(vectorized: Word2VectWithModel): Unit = {
    while (true) {
      val userinput = readLine
      try {
        val res = vectorized.model.findSynonyms(userinput, 5)
        res.show(5)
      } catch {
        case e: IllegalStateException => println(e)
      }

    }
  }

  def getTopAuthorsFromNumberOfPosts(df: DataFrame, numberOfPosts: Int): DataFrame = {
    import sqlContext.implicits._
    df.rdd.map(row => (row.getAs[String]("author"),
      row.getAs[Seq[String]]("words"), row.getAs[Long]("createdTimestamp"), row.getAs[Long]("messageId"), row.getAs[String]("subject")))
      .groupBy(_._1)
      .map(x => (x._1, x._2, x._2.size))
      .sortBy(_._3, ascending = false)
      .filter(x => x._3 > numberOfPosts)
      .map(_._2.map(x => Message(x._2, x._1, x._3, x._4, x._5)))
      .flatMap(identity)
      .filter(_.words.nonEmpty)
      .toDF()
  }

}


object MysqlLoader {
  def main(args: Array[String]): Unit = {
    val sparkContext = SparkContextInitializer.createSparkContext("mysqlLoader", List.empty)
    val runConfig = RunConfig(2, 1, 200)
    new MysqlLoader(SQLContext.getOrCreate(sparkContext), runConfig).load()
  }

  def slice(input: Array[String], windowSize: Int): Array[Array[String]] = {
    loop(input, windowSize, ArrayBuffer.empty, 0)
  }

  def loop(input: Array[String], windowSize: Int, res: ArrayBuffer[Array[String]], index: Int): Array[Array[String]] = {
    if (windowSize + index < input.length) {
      res += input.slice(index * windowSize, index + 1 * windowSize)
      loop(input, windowSize, res, index + 1)
    } else {
      res.toArray
    }
  }


}