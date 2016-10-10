import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.{ MergeStrategy, PathList }

object BuildValhalla extends Build {
  lazy val id = "valhalla"
  lazy val projVersion = "0.0.1"
  lazy val projOrganization = "com.argcv"
  lazy val projScalaVersion = "2.11.8"

  lazy val commonSettings = Seq(
    name := id,
    version := projVersion,
    organization := projOrganization,
    scalaVersion := projScalaVersion,
    licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/yuikns/valhalla"))
  )

  lazy val publishSettings = Seq(
    isSnapshot := false,
    publishMavenStyle := true,
    //    publishTo := {
    //      val nexus = "https://oss.sonatype.org/"
    //      if (isSnapshot.value)
    //        Some("snapshots" at nexus + "content/repositories/snapshots")
    //      else
    //        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    //    },
    publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository"))),
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false }
  )

  lazy val dependenciesSettings = Seq(
    resolvers ++= Seq(
      "Atlassian Releases" at "https://maven.atlassian.com/public/",
      "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
      Resolver.sonatypeRepo("snapshots"),
      Classpaths.typesafeReleases,
      Classpaths.typesafeSnapshots
    ),
    libraryDependencies ++= Seq(
      "commons-pool" % "commons-pool" % "1.6", // pool for SockPool
      "net.liftweb" % "lift-webkit_2.11" % "3.0-M6", // a light weight framework for web
      "com.google.guava" % "guava" % "18.0", // string process etc. (snake case for example)
      "ch.qos.logback" % "logback-classic" % "1.1.2", // logger, can be ignored in play framwork
      "org.iq80.leveldb" % "leveldb" % "0.7", // leveldb
      "org.scalanlp" % "breeze_2.11" % "0.11.2", // collection
      "org.spire-math" % "spire_2.11" % "0.12.0",
      "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test"
    ),
    dependencyOverrides ++= Set(
      "org.scala-lang" % "scala-reflect" % projScalaVersion,
      "org.scala-lang" % "scala-compiler" % projScalaVersion,
      "org.scala-lang" % "scala-library" % projScalaVersion,
      "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
    )
  )

  lazy val assemblySettings = Seq(
    assemblyJarName in assembly := s"$id-$projVersion-$projScalaVersion.jar",
    assemblyMergeStrategy in assembly := {
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  lazy val root = Project(id = id, base = file("."))
    .settings(commonSettings: _*)
    .settings(publishSettings: _*)
    .settings(assemblySettings: _*)
    .settings(dependenciesSettings: _*)

}

