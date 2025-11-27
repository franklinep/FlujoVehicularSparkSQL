
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "FlujoVehicularPeaje",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.12.10",
      "org.apache.spark" %% "spark-core" % "3.0.1",
      "org.apache.spark" %% "spark-sql"  % "3.0.1",
      "org.postgresql" % "postgresql" % "42.2.20"
    ),
    Compile / run / fork := true
  )