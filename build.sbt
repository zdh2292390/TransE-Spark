name := "TransE-Spark"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-math3" % "3.5",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-graphx" % "1.6.0",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.databricks" %% "spark-csv" % "1.5.0"
)