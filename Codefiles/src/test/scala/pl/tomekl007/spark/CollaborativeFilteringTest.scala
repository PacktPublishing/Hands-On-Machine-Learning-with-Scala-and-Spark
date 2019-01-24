package pl.tomekl007.spark

import java.time.ZonedDateTime

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SQLContext
import org.scalatest.Matchers._

class CollaborativeFilteringTest extends SparkSuite {
  override def appName: String = "CF_TEST"

  test("should find recommendations using CF") {
    val res = CFReco.findRecommendedMovieForUserId(spark, "13")

    res._1.show()
  }

  test("should return high prediction that user will like movie that was liked by a similar user") {
    //given
    val spark = SparkContextInitializer.createSparkContext("test-CF", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        Rating(userId = 1, movieId = 1, rating = 5.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 1, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 1, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 2, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 3, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli)
      ))


    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(data)

    //will user 1 rate movie 2 high?
    val probabilityThatUserWillLikeMovieThatWasLikeBySimilarUser
    = model.transform(SQLContext.getOrCreate(spark).createDataFrame(
      List(Rating(userId = 1, movieId = 2, rating = 5.0F)))
    )

    probabilityThatUserWillLikeMovieThatWasLikeBySimilarUser.first().getAs[Float]("prediction") > 2.5 shouldBe true

  }

  test("should return low prediction that user will like a movie that was not liked by a similar user") {
    //given
    val spark = SparkContextInitializer.createSparkContext("test-CF", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        Rating(userId = 1, movieId = 1, rating = 5.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 1, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 1, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 2, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 3, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli)
      ))


    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(data)

    //when
    val probabilityThatUserWillLikeMovieThatWasNotLikedBySimilarUser
    = model.transform(SQLContext.getOrCreate(spark).createDataFrame(
      List(Rating(userId = 1, movieId = 3, rating = 5.0F)))
    )

    //then
    probabilityThatUserWillLikeMovieThatWasNotLikedBySimilarUser.first().getAs[Float]("prediction") < 1.0 shouldBe true

  }

  test("should return highest prediction for movie that user will probably like based on similiary with other user") {
    //given
    val spark = SparkContextInitializer.createSparkContext("test-CF", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        Rating(userId = 1, movieId = 1, rating = 5.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 1, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 1, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 2, rating = 4.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 3, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 4, rating = 5.0F, ZonedDateTime.now().toInstant.toEpochMilli),
        Rating(userId = 2, movieId = 5, rating = 1.0F, ZonedDateTime.now().toInstant.toEpochMilli)
      ))


    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(data)

    //when
    val predictionsIfUserWillLikeMovie
    = model.transform(SQLContext.getOrCreate(spark).createDataFrame(
      List(
        Rating(userId = 1, movieId = 2, rating = 5.0F),
        Rating(userId = 1, movieId = 3, rating = 5.0F),
        Rating(userId = 1, movieId = 4, rating = 5.0F))
    ))

    //then
    val predictionThatUserLikeMovie2 = predictionsIfUserWillLikeMovie.filter(predictionsIfUserWillLikeMovie("movieId").isin("2")).first.getAs[Float]("prediction")
    val predictionThatUserLikeMovie3 = predictionsIfUserWillLikeMovie.filter(predictionsIfUserWillLikeMovie("movieId").isin("3")).first.getAs[Float]("prediction")
    val predictionThatUserLikeMovie4 = predictionsIfUserWillLikeMovie.filter(predictionsIfUserWillLikeMovie("movieId").isin("4")).first.getAs[Float]("prediction")

    predictionThatUserLikeMovie4 > predictionThatUserLikeMovie2 shouldEqual true
    predictionThatUserLikeMovie2 > predictionThatUserLikeMovie3 shouldEqual true

  }


}