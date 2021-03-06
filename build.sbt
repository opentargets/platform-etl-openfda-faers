lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "io.opentargets",
      scalaVersion := "2.12.10",
    )),
    name := "openfda",
    version := "1.1.2",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,
    coverageHighlighting := true,
    libraryDependencies ++= dependencies,
    // Assembly plugin configuration for fat jar
    mainClass in assembly := Some(s"${organization.value}.${name.value}.Main"),
    assemblyJarName in assembly := s"io-opentargets-etl-backend-assembly-${name.value}-${version.value}.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true),
    // Use 'provided' dependencies when running locally (see https://github.com/sbt/sbt-assembly#-provided-configuration)
    run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated,
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    }

  )

//val sparkVer = "2.4.5"
val sparkVer = "3.1.1"
val scalaTestVer = "3.1.1"
lazy val dependencies = Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.github.scopt" %% "scopt" % "4.0.0-RC1",
  "com.github.pureconfig" %% "pureconfig" % "0.12.3",
  "org.apache.spark" %% "spark-core" % sparkVer % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVer % "provided",
  // ML library includes the org.scalanlp (Breeze) libraries. Use this library
  // to prevent compile/run-time dependency clashes.
  "org.apache.spark" %% "spark-mllib" % sparkVer % "provided",
  "org.scalatest" %% "scalatest" % scalaTestVer % "test"
)