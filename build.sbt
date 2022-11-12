name := "scio-sparql"

sonatypeCredentialHost := "s01.oss.sonatype.org"

organization := "es.jolivar"
homepage := Some(url("https://github.com/jordiolivares/scio-sparql"))

licenses := Seq(
  "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
)

developers := List(
  Developer(
    "jordiolivares",
    "Jordi Olivares Provencio",
    "jordi@jolivar.es",
    url("https://github.com/jordiolivares")
  )
)

scalaVersion := "2.13.8"

versionScheme := Some("semver-spec")

libraryDependencies ++= {
  val scioVersion = "0.11.7"
  val rdf4jVersion = "4.2.1"
  val circeVersion = "0.14.2"
  Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "org.eclipse.rdf4j" % "rdf4j-rio-rdfxml" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-model" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-query" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-queryalgebra-model" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-queryalgebra-evaluation" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-queryparser-sparql" % rdf4jVersion,
    "org.eclipse.rdf4j" % "rdf4j-storage" % rdf4jVersion % Test,
    "org.scalatest" %% "scalatest" % "3.2.12" % Test,
    "com.spotify" %% "scio-test" % scioVersion % Test,
    "io.circe" %% "circe-core" % "0.14.2" % Test,
    "io.circe" %% "circe-generic" % circeVersion % Test,
    "io.circe" %% "circe-parser" % circeVersion % Test
  )
}
