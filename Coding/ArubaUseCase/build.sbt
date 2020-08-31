name := "ArubaUseCase"

version := "0.1"

scalaVersion := "2.12.10"
//Defining a scala version here adds scala as a dependency in your project,
// which we don't want so we set the following property as false
autoScalaLibrary := false

val sparkVersion = "3.0.0-preview2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies