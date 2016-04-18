package com.concord.utils

import scala.util.parsing.combinator.RegexParsers
import java.util.Date
import java.util.Locale
import java.util.TimeZone
import java.text.SimpleDateFormat

case class SimpleDateParser(year: Integer, month: Integer, msg: String) {
  override def toString: String = ""
}

object SimpleDateParser {
  private val regex = """-\s\d+\s(\d+)\.\d+\.(\d+)\s\w+\s\w+\s\d+\s\d+:\d+:\d+\s\S+\s(.*)$""".r

  def parse(input: String): Option[SimpleDateParser] = input match {
    case regex(year, month, msg) => Some(SimpleDateParser(year.toInt, month.toInt, msg))
    case _ => None
  }
}

case class LogParser(timestamp: Integer,
                     username: String,
                     nodename: String,
                     msg: String) {
  override def toString: String = buildKey + "||" + buildValue
  def buildKey: Int = msg.hashCode + timestamp
  def buildValue: String = {
    val iso8601 = LogParser.dateFormat.format(new Date());
    val timeMs = timestamp * 1000
    s"$iso8601:$timeMs:$username@$nodename:$msg"
  }
}

object LogParser {
  private val regex = """-\s(\d+)(?:\s\S+){5}\s(\w+)(.)(\w+)\s(.*)$""".r
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US);
  dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

  def parse(input: String): Option[LogParser] = input match {
    case regex(timestamp, username, nodeChar, nodename, msg)
        => Some(LogParser(timestamp.toInt, username, nodename, msg))
    case _ => None
  }

}
