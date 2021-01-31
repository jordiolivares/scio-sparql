name := "scio-sparql"

organization := "es.jolivar"

version := "0.1.0"

scalaVersion := "2.13.4"

libraryDependencies ++= {
  val scioVersion = "0.9.6"
  Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.eclipse.rdf4j" % "rdf4j-storage" % "3.5.1",
    "org.scalatest" %% "scalatest" % "3.2.2" % Test,
    "com.spotify" %% "scio-test" % scioVersion % Test
  )
}
