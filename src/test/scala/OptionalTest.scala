import org.eclipse.rdf4j.rio.RDFFormat

class OptionalTest extends SparqlPipelineTest {
  "SPARQL queries with OPTIONAL" should "work correctly with a single optional" in {
    testSparql(
      "optionals.ttl",
      RDFFormat.TURTLE,
      """
      |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      |SELECT ?name ?mbox
      |WHERE  { ?x foaf:name  ?name .
      |         OPTIONAL { ?x  foaf:mbox  ?mbox }
      |       }""".stripMargin
    )
  }

  it should "work correctly with a multiple optionals" in {
    val query =
      """
        |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        |SELECT ?name ?mbox ?hpage
        |WHERE  { ?x foaf:name  ?name .
        |         OPTIONAL { ?x foaf:mbox ?mbox } .
        |         OPTIONAL { ?x foaf:homepage ?hpage }
        |       }""".stripMargin
    testSparql("optionals2.ttl", RDFFormat.TURTLE, query)
  }
}
