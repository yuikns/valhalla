import sbt.Keys._
import sbt._

object BuildValhalla extends Build {
  lazy val id = "valhalla" // valhalla

  lazy val commonSettings = Seq(
    name := id,
    version := "0.0.1",
    organization := "com.argcv",
    scalaVersion := "2.11.7",
    licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/yuikns/valhalla"))
  )

  lazy val publishSettings = Seq(
    isSnapshot := false,
    publishMavenStyle := true,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
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
      "org.scalanlp" % "breeze_2.11" % "0.11.2", // collection
      "org.scalatest" % "scalatest_2.11" % "2.2.5" % "test"
    ),
    dependencyOverrides ++= Set(
      "org.scala-lang" % "scala-reflect" % "2.11.7",
      "org.scala-lang" % "scala-compiler" % "2.11.7",
      "org.scala-lang" % "scala-library" % "2.11.7",
      "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
    )
  )

  lazy val root = Project(id = id, base = file("."))
    .settings(commonSettings: _*)
    .settings(publishSettings: _*)
    .settings(dependenciesSettings: _*)

}

