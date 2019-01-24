package pl.tomekl007.spark.model

import java.time.{ZoneOffset, Instant}
import java.time.ZonedDateTime._

import org.apache.spark.sql.Row


case class Message(words: Seq[String], author: String, createdTimestamp: Long, messageId: Long, subject: String)


//object Message {
//
//  def apply(row: Row): Message = {
//
//  }
//}