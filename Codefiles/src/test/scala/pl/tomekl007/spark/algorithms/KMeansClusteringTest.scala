package pl.tomekl007.spark.algorithms

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SQLContext
import pl.tomekl007.spark.{SparkContextInitializer, SparkSuite}

class KMeansClusteringTest extends SparkSuite {
  override def appName: String = "K-Means_test"

  test("should calculate cluster using K-means") {
    //given
    val spark = SparkContextInitializer.createSparkContext("k-means", List.empty)
    val data = SQLContext.getOrCreate(spark)
      .createDataFrame(List(
        Record(Vectors.dense(Array(131d, 11d))),
        Record(Vectors.dense(Array(112d, 113d))),
        Record(Vectors.dense(Array(100d, 44d))),
        Record(Vectors.dense(Array(233d, 55d))),
        Record(Vectors.dense(Array(512d, 12415d)))
      )).toDF()

    //when
    val res = KMeansClustering.apply(data)

    //then
    println(res.clusterCenters)
    println(res.k)
    println(res.predict(Vectors.dense(Array(123d, 12d))))

  }
}

case class Record(result: Vector)
