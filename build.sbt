name := "scio-sparql"

organization := "es.jolivar"

version := "0.1.0"

scalaVersion := "2.13.6"

libraryDependencies ++= {
  val scioVersion = "0.11.0"
  val rdf4jVersion = "3.7.1"
  val circeVersion = "0.14.1"
  Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.eclipse.rdf4j" % "rdf4j-rio-rdfxml" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-model" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-query" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-queryalgebra-model" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-queryalgebra-evaluation" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-queryparser-sparql" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-storage" % rdf4jVersion % Test,
    "org.scalatest" %% "scalatest" % "3.2.9" % Test,
    "com.spotify" %% "scio-test" % scioVersion % Test,
    "io.circe" %% "circe-core" % "0.14.1" % Test,
    "io.circe" %% "circe-generic" % circeVersion % Test,
    "io.circe" %% "circe-parser" % circeVersion % Test
  )
}
