package pl.tomekl007.spark.algorithms

import org.apache.spark.ml.feature.{IDF, HashingTF}
import org.apache.spark.sql.DataFrame

/**
  * Created by tomasz.lelek on 15/03/16.
  */
//https://en.wikipedia.org/wiki/Tf%E2%80%93idf
object TfIdf {
  def apply(words: DataFrame) = {
    val hashingTF = new HashingTF()
      .setInputCol("words")
      .setOutputCol("rawFeatures")
      .setNumFeatures(20)
    val featurizedData = hashingTF.transform(words)
    featurizedData.show(3)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.show(3)

  }
}
