name := "ClientStatus"

version := "0.1"

scalaVersion := "2.12.15"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"

// FlatSpec
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.3.0-SNAP2" % Test