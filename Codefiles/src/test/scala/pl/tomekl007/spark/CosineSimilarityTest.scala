package pl.tomekl007.spark

import org.scalatest.FunSuite
import pl.tomekl007.spark.algorithms.Word2VecImpl

class CosineSimilarityTest extends FunSuite {
  test("should procure cosine similarity") {
    val res = Word2VecImpl(List("Norway", "Sweden"), RunConfig()).dataFrame
    val resVectors = res.map(r => r.getAs[org.apache.spark.mllib.linalg.Vector]("result")).collect()
    val vect1 = resVectors(0).toArray.toVector
    val vect2 = resVectors(1).toArray.toVector
    println("Vect1: " + vect1)
    println("Vect2" + vect2)
    val result = CosineSimilarity(vect1, vect2)
    println("Res" + result)

  }

  test("should procure cosine similarity simple vect similar") {
    val vect1 = Vector(1d, 0.2, 0.3)
    val vect2 = Vector(1d, 0.2, 0.3)
    println("Vect1: " + vect1)
    println("Vect2" + vect2)
    val result = CosineSimilarity(vect1, vect2)
    assert(result > 0.99)
  }

  test("should procure cosine opposite simple vect similar") {
    val vect1 = Vector(0.1d, 0.1, 1d)
    val vect2 = Vector(1d, 1d, 0.1)
    println("Vect1: " + vect1)
    println("Vect2" + vect2)
    val result = CosineSimilarity(vect1, vect2)
    assert(result > 0.19)
  }
  /*
  The line vec.similarity("word1","word2") will return the cosine similarity of the two words you enter.
  The closer it is to 1, the more similar the net perceives those words to be (see the Sweden-Norway example above).
   For example:
       double cosSim = vec.similarity("day", "night");
       System.out.println(cosSim);
     //output: 0.7704452276229858
  */
  test("day and night should be similar") {
    val res = Word2VecImpl(List("day", "night"), RunConfig()).dataFrame
    val resVectors = res.map(r => r.getAs[org.apache.spark.mllib.linalg.Vector]("result")).collect()
    val vect1 = resVectors(0).toArray.toVector
    val vect2 = resVectors(1).toArray.toVector
    println("Vect1: " + vect1)
    println("Vect2" + vect2)
    val result = CosineSimilarity(vect1, vect2)
    println("Res" + result)

  }
}
