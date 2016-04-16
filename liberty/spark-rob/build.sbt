name := "spark-benchmark"

version := "0.0.1"

scalaVersion := "2.10.5"

scalacOptions ++= Seq("-feature", "-language:higherKinds")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "com.twitter" % "algebird-core_2.10" % "0.11.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
)

assemblyMergeStrategy in assembly := {
  case x if x.endsWith("project.clj") => MergeStrategy.discard // Leiningen build files
  case x if x.toLowerCase.startsWith("meta-inf") => MergeStrategy.discard // More bumf
  case _ => MergeStrategy.first
}
