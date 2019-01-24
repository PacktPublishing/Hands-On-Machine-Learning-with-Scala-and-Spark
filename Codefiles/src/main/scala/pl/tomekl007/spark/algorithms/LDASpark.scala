package pl.tomekl007.spark.algorithms

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.sql.DataFrame

/**
  * Created by tomasz.lelek on 18/03/16.
  */
object LDASpark {
  def apply(dataFrame: DataFrame) {
    val vect = dataFrame.rdd
      .map(row => row.getAs[org.apache.spark.mllib.linalg.Vector]("result"))
    // Index documents with unique IDs
    val corpus = vect.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
    val topics = ldaModel.topicsMatrix
    println("topics :  " + topics)
    for (topic <- Range(0, 3)) {
      print("Topic " + topic + ":")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(" " + topics(word, topic))
      }
      println()
    }
  }
}

