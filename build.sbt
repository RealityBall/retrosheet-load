name := "retrosheet-load"

lazy val commonSettings = Seq(
  organization := "org.bustos",
  version := "0.1.0",
  scalaVersion := "2.11.7"
)

lazy val commons = ProjectRef(file("../common"), "common")

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= projectLibraries)
  .dependsOn(commons)

val slf4jV = "1.7.6"

val projectLibraries = Seq(
  "com.typesafe.slick"   %%  "slick"                % "3.2.1",
  "com.github.tototoshi" %%  "scala-csv"            % "1.1.2",
  "log4j"                %   "log4j"                % "1.2.14",
  "org.slf4j"            %   "slf4j-api"            % slf4jV,
  "org.slf4j"            %   "slf4j-log4j12"        % slf4jV,
  "org.scalatest"        %%  "scalatest"            % "3.0.1" % "test",
  "io.spray"             %%  "spray-json"           % "1.3.1",
  "mysql"                %   "mysql-connector-java" % "5.1.23",
  "joda-time"            %   "joda-time"            % "2.7",
  "org.joda"             %   "joda-convert"         % "1.2"
)
