lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "io.opentargets",
      scalaVersion := "2.12.10",
    )),
    name := "openfda",
    version := "0.1.0",
    //    sparkVersion := "2.4.5",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled", "-Dlogback.configurationFile=logback.xml"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,
    coverageHighlighting := true,
    libraryDependencies ++= dependencies,
    // Assembly plugin configuration for fat jar
    mainClass in assembly := Some(s"${organization.value}.${name.value}.Main"),
    assemblyJarName in assembly := s"${name.value}-${version.value}.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    }

  )

val sparkVer = "2.4.5"
val scalaTestVer = "3.1.1"
lazy val dependencies = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.github.scopt" %% "scopt" % "4.0.0-RC1",
  "com.github.pureconfig" %% "pureconfig" % "0.12.3",
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  // ML library includes the org.scalanlp (Breeze) libraries. Use this library
  // to prevent compile/run-time dependency clashes.
  "org.apache.spark" %% "spark-mllib" % sparkVer,
  "org.scalactic" %% "scalactic" % scalaTestVer,
  "org.scalatest" %% "scalatest" % scalaTestVer % "test"
)