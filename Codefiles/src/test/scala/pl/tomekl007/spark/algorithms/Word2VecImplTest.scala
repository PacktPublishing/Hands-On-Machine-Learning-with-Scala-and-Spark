package pl.tomekl007.spark.algorithms

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import pl.tomekl007.spark.RunConfig

/**
  * Created by tomasz.lelek on 15/04/16.
  */
class Word2VecImplTest extends FunSuite {

  test("should return vector for sentence") {
    val vect = Word2VecImpl("to jest zdanie", RunConfig())

    vect.size > 1 shouldBe true

  }


  test("should return vector for sentence more eloquent") {
    val vect = Word2VecImpl("to jest zdanie bardziej zlozone w ktorym nie bedzie tyle stopwordsow", RunConfig())

    vect.size > 1 shouldBe true

  }

  test("should return vector for sentence more eloquent longer") {
    val vect = Word2VecImpl("p.t. \"Emerald Tears\" - pozycja raczej dla koneserów, gdyż nie kazdy uciągnie 45 minet na jednym instrumencie i do tego akurat kontrabasie...", RunConfig() )

    vect.size > 1 shouldBe true

  }

}
