package pl.tomekl007.spark

import java.time.ZonedDateTime

import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SQLContext}


object CFReco {
  def findRecommendedMovieForUserId(spark: SparkContext,
                                    userId: String,
                                    ratingsSupplier: (SparkContext) => DataFrame
                                    = fileRatingSupplier): (DataFrame, ALSModel) = {
    val (df, model) = createModel(spark, ratingsSupplier)
    (df.filter(df("userId").isin(userId)), model)
  }

  def fileRatingSupplier(spark: SparkContext): DataFrame = {
    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._

    spark.textFile(this.getClass.getResource("/sample_movielens_ratings.txt").getPath)
      .map(parseRating)
      .toDF()
  }


  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def createModel(spark: SparkContext, ratingsSupplier: (SparkContext) => DataFrame): (DataFrame, ALSModel) = {

    val ratings = ratingsSupplier.apply(spark)
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    (predictions, model)
  }
}

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long = ZonedDateTime.now().toInstant.getEpochSecond)