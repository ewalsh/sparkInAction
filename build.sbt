scalaVersion := "2.12.10"

sbtVersion := "1.5.2"

organization := "ai.economicdatasciences"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-mllib" % "3.1.2",
  "com.typesafe" % "config" % "1.4.2",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % Test
)
