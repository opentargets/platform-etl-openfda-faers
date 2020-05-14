lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "io.opentargets",
      scalaVersion := "2.12.10",
    )),
    name := "openFda",
    version := "0.1.0",
    //    sparkVersion := "2.4.5",
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled", "-Dlog4j.configuration=log4j.properties"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    parallelExecution in Test := false,
    fork := true,
    coverageHighlighting := true,
    libraryDependencies ++= dependencies,
    // Assembly plugin configuration for fat jar
    mainClass in assembly := Some(s"$organization.$name.Main"),
    assemblyJarName in assembly := s"${name.value}.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
      case PathList("javax", "inject", xs@_*) => MergeStrategy.last
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
      case PathList("javax", "activation", xs@_*) => MergeStrategy.last
      case PathList("org", "apache", xs@_*) => MergeStrategy.last
      case PathList("com", "google", xs@_*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
      case PathList("com", "codahale", xs@_*) => MergeStrategy.last
      case PathList("com", "yammer", xs@_*) => MergeStrategy.last
      case PathList("org", "slf4j", "impl", xs@_*) => MergeStrategy.last
      case "module-info.class" => MergeStrategy.last
      case "reference-overrides.conf" => MergeStrategy.concat
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" =>
        MergeStrategy.concat
      case "about.html" => MergeStrategy.rename
      case "overview.html" => MergeStrategy.rename
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }

  )

val sparkVer = "2.4.5"
val scalaTestVer = "3.1.1"
lazy val dependencies = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "com.github.scopt" %% "scopt" % "4.0.0-RC1",
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  "org.scalactic" %% "scalactic" % scalaTestVer,
  "org.scalatest" %% "scalatest" % scalaTestVer % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test"
)


