package pl.tomekl007.spark.algorithms.logistic_regression

import org.apache.spark.mllib.clustering.{GaussianMixture, GaussianMixtureModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.joda.time.{DateTime, DateTimeZone}

object GMMWrapper {
  def apply(dataFrame: DataFrame, author: String, numberOfClusters: Int = 2): GaussianMixtureModel = {
    // Load and parse the data
    val timeOfPostsForAuthor = dataFrame.rdd
      .map(row => (row.getAs[String]("author"), row.getAs[Long]("createdTimestamp")))
      .map(r => (r._1, normalizeTimeAndGetHour(r)))
      .filter(a => a._1.equalsIgnoreCase(author))
      .groupByKey()

    println(s"times of posts for author: $author")
    timeOfPostsForAuthor.values.flatMap(identity).groupBy(v => v).map(v => (v._1, v._2.size)).sortBy(v => v._2).foreach(println)

    val parsedData = timeOfPostsForAuthor
      .flatMap(_._2)
      .map(elem => Vectors.dense(elem))
      .cache()

    // Cluster the data into classes using GaussianMixture
    val gmm = new GaussianMixture()
      .setK(numberOfClusters) //experiment with number of gaussian
      .run(parsedData)

    // output parameters of max-likelihood model
    for (i <- 0 until gmm.k) {
      println("weight=%f\nmu=%s\nsigma=\n%s\n" format
        (gmm.weights(i), gmm.gaussians(i).mu, gmm.gaussians(i).sigma))
    }
    gmm
  }

  def normalizeTimeAndGetHour(r: (String, Long)): Double =
    normalizeTimeAndGetHour(r._2)

  def normalizeTimeAndGetHour(time: Long): Double =
    new DateTime(time, DateTimeZone.UTC).getHourOfDay
}
