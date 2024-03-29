name := "scio-sparql"

organization := "es.jolivar"

version := "0.1.0"

scalaVersion := "2.13.5"

libraryDependencies ++= {
  val scioVersion = "0.10.0"
  Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.eclipse.rdf4j" % "rdf4j-rio-rdfxml" % "3.6.0",
    "org.eclipse.rdf4j" % "rdf4j-model" % "3.6.0",
    "org.eclipse.rdf4j" % "rdf4j-query" % "3.6.0",
    "org.eclipse.rdf4j" % "rdf4j-queryalgebra-model" % "3.6.0",
    "org.eclipse.rdf4j" % "rdf4j-queryalgebra-evaluation" % "3.6.0",
    "org.eclipse.rdf4j" % "rdf4j-queryparser-sparql" % "3.6.0",
    "org.eclipse.rdf4j" % "rdf4j-storage" % "3.6.0" % Test,
    "org.scalatest" %% "scalatest" % "3.2.2" % Test,
    "com.spotify" %% "scio-test" % scioVersion % Test,
    "io.circe" %% "circe-core" % "0.13.0" % Test,
    "io.circe" %% "circe-generic" % "0.13.0" % Test,
    "io.circe" %% "circe-parser" % "0.13.0" % Test
  )
}
