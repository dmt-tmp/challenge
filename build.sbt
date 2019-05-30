import sys.process._

name := "challenge"
version := "0.1-SNAPSHOT"
organization := "jp.paypay"
// scalafmtConfig := Some(new File("scalafmt-conf/.scalafmt.conf"))

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature", // More verbose warnings
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused",
  "-Ywarn-unused-import" // scala 2.11+ only
)

val sparkVersion = "2.2.3"

// Exclude slf4j from spark testing base that changes spark log level and spams the terminal.
// val excludeSlf4jLog4j = ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")

libraryDependencies ++= Seq(
 // Spark dependencies. Marked as provided because they must not be included in the uber jar
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  // "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // "org.apache.hadoop" % "hadoop-aws" % "2.7.3" % "provided",

  // Test libraries
  "org.scalatest" %% "scalatest" % "3.0.2" % Test,
  // "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.4" % "test", // excludeAll excludeSlf4jLog4j,
  "com.holdenkarau" %% "spark-testing-base" % "2.2.3_0.12.0" % Test
)

// Exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)


// Disable parallel execution because of spark-testing-base
Test / parallelExecution := false

// Configure the build to publish the assembly JAR
Compile / assembly / artifact := {
  val art = (Compile / assembly / artifact).value
  art.withClassifier(Some("assembly"))
}
addArtifact(Compile / assembly / artifact, assembly)

// Forking is needed to change the JVM options
Test / fork := true
// Minimum memory requirements for spark-testing-base
Test / javaOptions ++= {
  val javaVersion: String = System.getProperty("java.version")
  if (javaVersion.startsWith("1.7"))
    Seq("-Xms256M", "-Xmx1024M", "-XX:MaxPermSize=1024M")
  else
    Seq("-Xms256M", "-Xmx1024M", "-XX:MaxMetaspaceSize=1024M")
}
