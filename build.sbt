name := "Akka HLL sets"

version := "0.0.1"

scalaVersion := "2.10.4"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4-SNAPSHOT"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4-SNAPSHOT"

libraryDependencies += "com.twitter" %% "algebird-core" % "0.7.1"

libraryDependencies += "org.apache.kafka" %% "kafka" %"0.8.1" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.6.6"

libraryDependencies += "org.msgpack" % "msgpack" % "0.6.11"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

