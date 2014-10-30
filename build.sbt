name := "Akka HLL sets"

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4-SNAPSHOT"

libraryDependencies += "com.twitter" %% "algebird-core" % "0.7.1"
