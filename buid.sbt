val buildResolvers = Seq(
  //    "Local Maven Repository"    at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  //    "Maven repository"          at "http://download.java.net/maven/2/",
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)

lazy val scalaLoggingDep = "ch.qos.logback" % "logback-classic" % "1.2.3"
lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.0"

lazy val sparkSeq = Seq(
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly (),
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-graphx" % "2.4.5",
  "org.apache.spark" %% "spark-mllib" % "2.4.5"
)
lazy val ammonite = "com.lihaoyi" % "ammonite" % "2.0.4" % "test" cross CrossVersion.full
//lazy val ammoniteDeps = Seq(
//  "com.github.pathikrit" %% "better-files" % "3.8.0",
//  "com.typesafe.play" %% "play-json" % "2.7.3",
//  "com.typesafe" % "config" % "1.3.4"
//)

lazy val root = (project in file("."))
  .settings(
    inThisBuild(
      List(
        organization := "io.opentargets",
        scalaVersion := "2.12.10",
        version := "0.1.0"
      )),
    name := "io-opentargets-etl-openfda-faers",
    // libraryDependencies += scalaTest % Test,

    // resolvers += Resolver.mavenLocal,
    // resolvers += Resolver.sonatypeRepo("releases"),
    resolvers ++= buildResolvers,
    libraryDependencies += scalaCheck,
    libraryDependencies ++= sparkSeq,
    libraryDependencies += scalaLoggingDep,
    libraryDependencies += scalaLogging,
    libraryDependencies += ammonite,
    testFrameworks += new TestFramework("minitest.runner.Framework"),
    assemblyMergeStrategy in assembly := {
      case PathList("org", "aopalliance", xs @ _*)      => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*)         => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*)        => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*)     => MergeStrategy.last
      case PathList("org", "apache", xs @ _*)           => MergeStrategy.last
      case PathList("com", "google", xs @ _*)           => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*)         => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*)           => MergeStrategy.last
      case PathList("org", "slf4j", "impl", xs @ _*)    => MergeStrategy.last
      case "about.html"                                 => MergeStrategy.rename
      case "overview.html"                              => MergeStrategy.rename
      case "plugin.properties"                          => MergeStrategy.last
      case "log4j.properties"                           => MergeStrategy.last
      case "git.properties"                             => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )
