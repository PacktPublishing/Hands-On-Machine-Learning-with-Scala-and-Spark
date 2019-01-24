package pl.tomekl007.spark.algorithms

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

/**
  * Created by tomasz.lelek on 16/03/16.
  */
object FPGrowth {
  def apply(data: RDD[Array[String]]) {
    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)

    val model = fpg.run(data)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)

    }
  }
}
