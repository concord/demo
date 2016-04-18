package com.concord.utils

import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}
import scala.collection.JavaConversions._

/**
  * Created by adev on 4/4/16.
  */
class SparkArgHelper(args: Array[String]) {

  object CliArgs {
    @Option(name = "-master", usage = "Spark master url")
    var masterUrl: String = "local[*]"

    @Option(name = "-kafka.metadata.broker.list", usage = "kafka broker info")
    var kafkaBrokers: String = ""

    @Option(name = "-kafka.topics", usage = "kafka topics separated by ,")
    var kafkaTopics: String = "liberty"

    @Option(name = "output", usage = "kafka, cassandra, or text")
    var output: String = "text"

    @Option(name = "outputDetails", usage = "kafka info, cassandra info, or text file location")
    var outputDetails: String = "/tmp/sample.txt"

    @Option(name = "-cassandra_hosts", usage = "10.2.0.0,10,23.32.1")
    var cassandraHosts: String = "127.0.0.1"

    @Option(name = "-cassandra_keyspace", usage = "keyspace name")
    var cassandraKeyspace: String = "test_keyspace"

    @Option(name = "-cassandra_table", usage = "irq")
    var cassandraTable: String = "irq"
  }

  val parser = new CmdLineParser(CliArgs)
  try {
    parser.parseArgument(args.toList)
  } catch {
    case e: CmdLineException =>
      print(s"Error:${e.getMessage}\n Usage:\n")
      parser.printUsage(System.out)
      System.exit(1)
  }
}
