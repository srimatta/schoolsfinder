
name := "schoolsfinder"

version := "0.1"

scalaVersion := "2.12.0"

val sparkVersion = "3.0.1"
val typeSafeConfigVersion = "1.3.0"
val scalatestVersion = "3.0.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % typeSafeConfigVersion,
  "org.scalatest" % "scalatest_2.12" % scalatestVersion % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}