package pl.tomekl007.spark.utils

import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import pl.tomekl007.spark.model.Message

import scala.annotation.tailrec

/**
  * Created by tomasz.lelek on 26/03/16.
  */
object InputCleanUtil {
  def containsOnlyLetters(s: String): Boolean = s.count(java.lang.Character.isLetter).equals(s.length)


  @tailrec
  def replaceLoop(x: String, charsToRemove: List[String]): String = {
    charsToRemove match {
      case Nil => x
      case head :: tail => replaceLoop(x.replace(head, ""), tail)
    }
  }


  def clearWords(words: Seq[String]): Seq[String] = {
    words.map(_.toLowerCase().trim)
      .filterNot(_.isEmpty)
      .filterNot(_.startsWith(">"))
      .filterNot(_.contains("-"))
      .filter(s => InputCleanUtil.containsOnlyLetters(s))
      .map(_.replaceAll("\\<[^>]*>", ""))
      .map(InputCleanUtil.replaceLoop(_, StopWords.forumCharsToRemove))
  }


  def tokenizeAndStopWords(dataframe_mysql: DataFrame, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val tokenizer = new Tokenizer().setInputCol("body").setOutputCol("words")
    val words = tokenizer.transform(dataframe_mysql)

    val stopWordsRemover = new StopWordsRemover().setInputCol("words").setOutputCol("without-stop-word")
    stopWordsRemover.setStopWords(StopWords.allStopWords)

    val withoutStopWords = stopWordsRemover.transform(words)

    cleanForumSpecific(withoutStopWords).toDS().toDF()
  }


  def cleanForumSpecific(withoutStopWords: DataFrame): RDD[Message] = {
    withoutStopWords.rdd
      .map(row => Message(row.getAs[Seq[String]]("without-stop-word"), row.getAs[String]("author"),
        row.getAs[Long]("datestamp"), row.getAs[Long]("message_id"), row.getAs[String]("subject")))
      .filter(m => m.words.nonEmpty)
      .filter(m => !m.author.equalsIgnoreCase("Anonymous user"))
      .map(r => Message(InputCleanUtil.clearWords(r.words).filter(_.length > 2),
        r.author, toMillis(r), r.messageId, r.subject)
      )
  }

  def toMillis(r: Message): Long = r.createdTimestamp * 1000

}
