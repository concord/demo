name := "spark-benchmark"

organization:= "concordsystems"

version := "0.0.1"

scalaVersion := "2.10.5"

scalacOptions ++= Seq("-feature", "-language:higherKinds")

resolvers ++= Seq(
  "Clojars" at "https://clojars.org/repo/",
  "Conjars" at "http://conjars.org/repo/",
  "Maven-Repository.com" at "http://repo1.maven.org/maven2/",
  "Central" at "http://central.maven.org/maven2/",
  "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.1" % "provided",
  "args4j" % "args4j" % "2.33",
  "com.twitter" % "algebird-core_2.10" % "0.11.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"
)

mergeStrategy in assembly := {
  case x if x.endsWith("project.clj") => MergeStrategy.discard // Leiningen build files
  case x if x.toLowerCase.startsWith("meta-inf") => MergeStrategy.discard // More bumf
  case _ => MergeStrategy.first
}
