ThisBuild / name := "http.almaren"
ThisBuild / organization := "com.github.music-of-the-ainur"

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.10"

crossScalaVersions := Seq(scala211,scala212)
ThisBuild / scalaVersion := scala212

val sparkVersion = "2.4.0"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "com.github.music-of-the-ainur" %% "almaren-framework" % "0.9.0-2.4" % "provided",
  "com.lihaoyi" %% "requests" % "0.1.9",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

enablePlugins(GitVersioning)

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/modakanalytics/http.almaren"),
    "scm:git@github.com:modakanalytics/http.almaren.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "mantovani",
    name  = "Daniel Mantovani",
    email = "daniel.mantovani@modak.com",
    url   = url("https://github.com/music-of-the-ainur")
  ),
 Developer(
    id    = "praveenkumarb1207",
    name  = "Praveen Kumar",
    email = "praveen.bachu@modak.com",
    url   = url("https://github.com/music-of-the-ainur")
  )
)

ThisBuild / description := "HTTP Connector For Almaren Framework"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/modakanalytics/http.almaren"))
ThisBuild / organizationName := "Modak Analytics"
ThisBuild / organizationHomepage := Some(url("https://github.com/modakanalytics"))


// Remove all additional repository other than Maven Central from POM
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true
updateOptions := updateOptions.value.withGigahorse(false)
