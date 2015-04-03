name := "retrosheet-load"

version := "1.0"

scalaVersion := "2.11.4"

mainClass in Compile := Some("org.bustos.realityball.RetrosheetLoad")

libraryDependencies ++= List(
  "com.typesafe.slick"   %%  "slick"                % "2.1.0",
  "com.github.tototoshi" %%  "scala-csv"            % "1.1.2",
  "log4j"                %   "log4j"                % "1.2.14",
  "org.slf4j"            %   "slf4j-api"            % "1.7.6",
  "org.slf4j"            %   "slf4j-log4j12"        % "1.7.6",
  "org.scalatest"        %%  "scalatest"            % "2.1.6" % "test",
  "io.spray"             %%  "spray-json"           % "1.3.1",
  "mysql"                %   "mysql-connector-java" % "latest.release",
  "joda-time"            %   "joda-time"              % "2.7",
  "org.joda"             %   "joda-convert"           % "1.2"
)
