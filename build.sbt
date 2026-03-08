ThisBuild / version := "1.0.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "scala-sbt"
  )

val sparkVersion = "3.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.iceberg" % "iceberg-spark-runtime-3.4_2.13" % "1.10.1",
  "org.apache.iceberg" % "iceberg-aws-bundle" % "1.10.1",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262"
)

