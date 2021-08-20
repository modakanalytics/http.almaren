name := "http-to-postgres-example"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "com.github.music-of-the-ainur" % "almaren-framework_2.11" % "0.9.0-2.4",
  "com.github.music-of-the-ainur" % "http-almaren_2.11" % "1.2.0-2.4",
  "org.postgresql" % "postgresql" % "42.2.23"
)
