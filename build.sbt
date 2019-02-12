name := "SparkDataPipeline"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"
val catsVersion = "1.6.0"

lazy val projectDeps = Seq(
  //TODO Add Provided before Uber-ing
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided ,
  "org.apache.hadoop" % "hadoop-common" % "2.8.3" % Provided,
  "com.google.guava" % "guava" % "14.0.1",
  "org.typelevel" %% "cats-core" % catsVersion withSources(),
  "org.typelevel" %% "cats-kernel" % catsVersion withSources(),
  "org.typelevel" %% "cats-macros" % catsVersion withSources(),
  "com.github.pureconfig" %% "pureconfig" % "0.9.2",
  "com.databricks" %% "spark-redshift" % "3.0.0-preview1",
  "com.amazon.redshift" % "redshift-jdbc42" % "1.2.15.1025",
  "com.amazonaws" % "aws-java-sdk" % "1.11.447",
  "org.apache.hadoop" % "hadoop-aws" % "2.8.3" exclude("com.amazonaws", "aws-java-sdk") exclude ("org.apache.hadoop", "hadoop-common"),
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalacheck" %% "scalacheck" % "1.13.4" % Test
)

libraryDependencies ++= projectDeps

dependencyOverrides += "com.databricks" %% "spark-avro" % "3.2.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

scalacOptions ++= Seq("-Ypartial-unification", "-unchecked", "-deprecation", "-feature", "-target:jvm-1.8", "-Ywarn-unused-import")
scalacOptions in(Compile, doc) ++= Seq("-unchecked", "-deprecation", "-diagrams", "-implicits")

test in assembly := {}

fork in Test := true
javaOptions ++= Seq("-Xms2g", "-Xmx3g", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := true


resolvers += "redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release"
resolvers += "jitpack" at "https://jitpack.io"
