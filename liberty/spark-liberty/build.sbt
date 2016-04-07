name := "spark"
organization:= "concordsystems"
version := "0.1"
scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Clojars" at "https://clojars.org/repo/",
  "Conjars" at "http://conjars.org/repo/",
  "Maven-Repository.com" at "http://repo1.maven.org/maven2/",
  "Central" at "http://central.maven.org/maven2/",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.1",
  "org.apache.spark" % "spark-streaming_2.11" % "1.6.1",
  "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.1",
  "args4j" % "args4j" % "2.33"
)

mergeStrategy in assembly := {
  case x if x.endsWith("project.clj") => MergeStrategy.discard // Leiningen build files
  case x if x.toLowerCase.startsWith("meta-inf") => MergeStrategy.discard // More bumf
  case _ => MergeStrategy.first
}
