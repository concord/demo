package io.concord.eval

// Thanks Anatoly
case class LogLine(
                    tsSecs: Int,
                    fullLine: String,
                    message: String
                  )

object LineParser {
  // Thanks Alex
  private[this] val lineRegex =
    """-\s(\d+)\s\d+\.\d+\.\d+\s\w+\s\w+\s\d+\s\d+:\d+:\d+\s\S+\s(.*)$""".r

  def apply(line: String): Option[LogLine] = {
    lineRegex.findFirstMatchIn(line).map { mtch =>
      val ts = mtch.group(1).toInt
      val message = mtch.group(2)
      LogLine(ts, line, message)
    }
  }
}
