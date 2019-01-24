name := "hands-on-ML"

lazy val root = (project in file(".")).settings(
  resolvers ++= Seq(
    "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
    "Java.net Maven2 Repository" at "http://download.java.net/maven/2/",
    "Central" at "https://repo1.maven.org/maven2"
  ),
  version := "0.1-SNAPSHOT",
  organization := "pl.tomekl007",
  scalacOptions in Compile ++= Seq(
    "-encoding", "UTF-8",
    "-target:jvm-1.7",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xlog-reflective-calls",
    "-Xlint"),
  javacOptions in Compile ++= Seq(
    "-source", "1.8",
    "-target", "1.8",
    "-Xlint:unchecked",
    "-Xlint:deprecation"),
  scalaVersion := "2.10.5",
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "2.2.5" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.1" % "test",
    "org.apache.spark" %% "spark-core" % "1.5.0",
    "org.apache.spark" %% "spark-sql" % "1.5.2",
    "org.apache.spark" %% "spark-mllib" % "1.6.1",
    "mysql" % "mysql-connector-java" % "8.0.13",
    "com.typesafe" % "config" % "1.2.0",
    "commons-logging" % "commons-logging" % "1.2",
    "org.scalaequals" %% "scalaequals-core" % "1.2.0",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" artifacts (Artifact("stanford-corenlp", "models"), Artifact("stanford-corenlp")),
    "edu.stanford.nlp" % "stanford-parser" % "3.6.0"
  ),

  libraryDependencies ++= Seq(("org.slf4j" % "slf4j-log4j12" % "1.7.10")
    .excludeAll(ExclusionRule(organization = "log4j"))),
  libraryDependencies += "log4j" % "log4j" % "1.2.16" % "test"
)

