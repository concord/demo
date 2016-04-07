resolvers in ThisBuild ++= Seq(Resolver.mavenLocal)

name := "Flink Eval"

version := "0.1-SNAPSHOT"

organization := "io.concord"

scalaVersion in ThisBuild := "2.11.8"

val flinkVersion = "1.0.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka-0.8" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "joda-time" % "joda-time" % "2.9.3"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
    fork in run := true,
    javaOptions in run += "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=localhost:8000"
  )

mainClass in assembly := Some("io.concord.jobs.WordCountJob")

mergeStrategy in assembly := {
  case x if x.endsWith("project.clj") => MergeStrategy.discard // Leiningen build files
  case x if x.toLowerCase.startsWith("meta-inf") => MergeStrategy.discard // More bumf
  case _ => MergeStrategy.first
}
