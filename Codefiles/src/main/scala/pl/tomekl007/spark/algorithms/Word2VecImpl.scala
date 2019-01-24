package pl.tomekl007.spark.algorithms

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{DataFrame, SQLContext}
import pl.tomekl007.spark.utils.InputCleanUtil
import pl.tomekl007.spark.{RunConfig, SparkContextInitializer}


/**
  * Created by tomasz.lelek on 15/03/16.
  */

case class Word2VectWithModel(dataFrame: DataFrame, model: Word2VecModel)

object Word2VecImpl {

  def apply(sentence: String, runConfig: RunConfig): Vector = {
    val spark = SparkContextInitializer.createSparkContext("Word2VectImpl", List.empty)
    val sqlContext = SQLContext.getOrCreate(spark)
    import sqlContext.implicits._
    val sentenceDf =
      InputCleanUtil
        .tokenizeAndStopWords(
          spark.parallelize(List(sentence), 2).map(InputBody(_)).toDF(), sqlContext)
    sentenceDf.show()
    val vectFromSentence = apply(sentenceDf, runConfig).dataFrame.rdd.map(row => row.getAs[Vector]("result")).first()

    println(vectFromSentence)
    vectFromSentence
  }

  def apply(words: List[String], runConfig: RunConfig): Word2VectWithModel = {
    val spark = SparkContextInitializer.createSparkContext("Word2VectImpl", List.empty)
    val sqlContext = SQLContext.getOrCreate(spark)
    import sqlContext.implicits._
    apply(spark.parallelize(words, 2).repartition(16).map(x => Input(Seq(x))).toDF(), runConfig)
  }

  case class InputBody(body: String, author: String = "ignore", datestamp: Long = 0,
                       message_id: Long = 0, subject: String = "Ignore") {
    def apply(body: String) = InputBody(body)
  }

  case class Input(words: Seq[String])


  //dataFrame should have .get("words") = Array[String]
  def apply(dataFrame: DataFrame, runConfig: RunConfig): Word2VectWithModel = {
    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("words")
      .setOutputCol("result")
      .setVectorSize(runConfig.W2VVectSize)
      .setMinCount(runConfig.W2VMinCount) //minWordFrequency is the minimum number of times a word must appear in the corpus. Here, if it appears less than 5 times, it is not learned.
    val model = word2Vec.fit(dataFrame)
    val result = model.transform(dataFrame)
    result.show(20)
    Word2VectWithModel(result, model)
  }
}
