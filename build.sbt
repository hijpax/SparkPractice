ThisBuild / version := "0.2.0"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "SparkPractice"
  )

val sparkVersion = "3.2.2"
val postgresVersion = "42.3.6"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.18.0" % "provided",
  "org.apache.logging.log4j" % "log4j-core" % "2.18.0" % "provided",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)
