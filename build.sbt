import sbt.Keys.libraryDependencies

name:= "dynamosparkv2"
version := "0.1"
organization := "com.goibibo"
scalaVersion := "2.12.8"
val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  //"org.json4s" % "json4s-jackson_2.11" % "3.5.0",
  // "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.129"
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.623",
  // https://mvnrepository.com/artifact/org.scala-lang.modules/scala-java8-compat
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.0",
  "com.amazonaws" % "aws-java-sdk-sts" % "1.11.571", 
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.571",
  "com.google.guava" % "guava" % "14.0.1" % "provided"


)