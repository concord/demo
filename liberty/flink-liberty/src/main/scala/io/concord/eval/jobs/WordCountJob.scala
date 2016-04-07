package io.concord.eval.jobs

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import io.concord.eval.LineParser

import org.joda.time.DateTime
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountJob extends App {
  case class GroupParams(month: Int, year: Int, word: String)

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val params = ParameterTool.fromArgs(args)

  val messageStream = env.addSource(new FlinkKafkaConsumer08(
    params.getRequired("topic"),
    new SimpleStringSchema,
    params.getProperties))

  val wordStream = messageStream.flatMap { line =>
    LineParser(line).toIterable
  }.assignAscendingTimestamps { line =>
    line.tsSecs.toLong * 1000
  }.flatMap { parsed =>
    val ts = new DateTime(parsed.tsSecs.toLong * 1000)
    parsed.message.split(" ").map { word =>
      (GroupParams(ts.monthOfYear().get, ts.year().get, word), 1)
    }
  }
    .keyBy { _._1 }
    .timeWindow(Time.seconds(10))
    .reduce { (lhs, rhs) =>
    (lhs._1, lhs._2 + rhs._2)
  }.print

  env.execute(this.getClass.getName)
}
