package pl.tomekl007.spark.algorithms

import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.rdd.RDD

/**
  * Created by tomasz.lelek on 16/03/16.
  */
object PrefixSpan {
  def apply(sequences: RDD[Array[Array[String]]]): Unit = {

    println("rdd.size : " + sequences.collect().length)

    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.01)
      .setMaxPatternLength(1000)

    val model = prefixSpan.run(sequences)
    model.freqSequences.foreach(println)
    model.freqSequences.collect().sortBy(_.freq).foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
  }
}
