spName := "spark-arrow"

name := "spark-arrow"

version := "1.0"

scalaVersion := "2.11.8"

sparkVersion := "2.0.1"

sparkComponents ++= Seq("sql")

libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "0.1.0",
  "org.apache.arrow" % "arrow-memory" % "0.1.0"

)

