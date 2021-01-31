import org.eclipse.rdf4j.rio.RDFFormat

class UnionTest extends SparqlPipelineTest {
  "SPARQL queries with UNION" should "work correctly with a single union" in {
    val query =
      """
        |PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
        |PREFIX dc11:  <http://purl.org/dc/elements/1.1/>
        |
        |SELECT ?title
        |WHERE  { { ?book dc10:title  ?title } UNION { ?book dc11:title  ?title } }""".stripMargin
    testSparql("union.ttl", RDFFormat.TURTLE, query)
  }

  it should "work correctly with other unions" in {
    val query =
      """
        |PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
        |PREFIX dc11:  <http://purl.org/dc/elements/1.1/>
        |
        |SELECT ?x ?y
        |WHERE  { { ?book dc10:title ?x } UNION { ?book dc11:title  ?y } }
        |""".stripMargin
    testSparql("union.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with more than one triple pattern in each union" in {
    val query =
      """
        |PREFIX dc10:  <http://purl.org/dc/elements/1.0/>
        |PREFIX dc11:  <http://purl.org/dc/elements/1.1/>
        |
        |SELECT ?title ?author
        |WHERE  { { ?book dc10:title ?title .  ?book dc10:creator ?author }
        |         UNION
        |         { ?book dc11:title ?title .  ?book dc11:creator ?author }
        |       }""".stripMargin
    testSparql("union.ttl", RDFFormat.TURTLE, query)
  }
}
