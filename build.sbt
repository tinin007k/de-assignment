name := "HpTaxiProblem"

version := "0.1"

scalaVersion := "2.12.6"

val sparkVersion = "2.4.0"


libraryDependencies +="com.typesafe" % "config" % "1.3.2"
libraryDependencies +="org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies +="org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies +="org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies +="org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies +="com.holdenkarau" %% "spark-testing-base" % "2.4.0_0.12.0" % "test"