name := "retrosheet-load"

version := "1.0"

scalaVersion := "2.11.4"

mainClass in Compile := Some("org.bustos.RetrosheetLoad.RetrosheetLoad")

libraryDependencies ++= List(
  "com.typesafe.slick" %% "slick" % "2.1.0",
  "org.slf4j"           % "slf4j-nop" % "1.6.4",
  "com.h2database"      % "h2" % "1.3.175",
  "org.scalatest"      %% "scalatest" % "2.1.6" % "test",
  "io.spray"           %%  "spray-json"    % "1.3.1",
  "mysql"               % "mysql-connector-java" % "latest.release"
)
