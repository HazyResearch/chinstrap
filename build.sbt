name := "DunceCap"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
 "org.scala-lang" % "scala-compiler" % "2.10.2",
 "org.scala-lang" % "jline" % "2.10.2",
 "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test",
 "org.apache.commons" % "commons-lang3" % "3.1",
 "io.argonaut" %% "argonaut" % "6.0.4",
 "org.apache.commons" % "commons-math3" % "3.2"
)