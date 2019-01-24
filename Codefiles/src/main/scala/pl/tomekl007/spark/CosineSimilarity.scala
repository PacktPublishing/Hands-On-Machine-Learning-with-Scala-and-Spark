package pl.tomekl007.spark

/**
  * Created by tomasz.lelek on 18/03/16.
  */
object CosineSimilarity {
  def apply(vectorA: Vector[Double], vectorB: Vector[Double]): Double = {
    var dotProduct = 0.0
    var normA = 0.0
    var normB = 0.0
    for (i <- 1 to vectorA.length - 1) {
      dotProduct += vectorA(i) * vectorB(i)
      normA += Math.pow(vectorA(i), 2)
      normB += Math.pow(vectorB(i), 2)
    }
    dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  }

}
