name := "sparkInAction"
sbtVersion := "1.5.2"
scalaVersion := "2.12.10"
organization := "ai.economicdatasciences"
version := "0.1-SNAPSHOT"

scalacOptions ++= Seq("-feature", "-language:postfixOps")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-mllib" % "3.1.2",
  "com.typesafe" % "config" % "1.4.2",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP3" % Test
)
