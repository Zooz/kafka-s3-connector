lazy val root = (project in file(".")).settings(
  name := "kafkas3connector",
  scalaVersion := "2.12.8",
  version := "0.9-SNAPSHOT",
  maintainer := "Noam Cohen <noam.cohen@zooz.com>, Guy Biecher <guy.biecher@zooz.com>"
).enablePlugins(PlayScala)

scalacOptions ++= Seq("-feature", "-deprecation")
  
libraryDependencies ++= Seq(
  guice,
  "org.apache.kafka" % "kafka-clients" % "1.1.0",
  "com.typesafe" % "config" % "1.3.3",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.515",
  "com.beachape" %% "enumeratum" % "1.5.13",
  "com.github.pathikrit" %% "better-files" % "3.6.0",
  "org.apache.commons" % "commons-compress" % "1.18",
  "org.joda" % "joda-convert" % "1.8.1",
  "io.prometheus" % "simpleclient" % "0.5.0",
  "io.prometheus" % "simpleclient_hotspot" % "0.5.0",
  "io.prometheus" % "simpleclient_httpserver" % "0.5.0",
  "io.prometheus" % "simpleclient_pushgateway" % "0.5.0",
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test,
  "org.mockito" %% "mockito-scala" % "0.3.1" % Test,
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.kafka" %% "kafka" % "1.1.0" % Test,
  "org.apache.curator" % "curator-test" % "4.0.1" % Test,
  "com.typesafe.akka" %% "akka-testkit" % "2.5.0" % Test
)

dependencyOverrides += "com.google.guava" % "guava" % "27.0-jre"
dependencyOverrides +=   "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0"

// Running unit tests one after the other to avoid conflicts with Cassandra/Kafka/S3 mocks
parallelExecution in Test := false
