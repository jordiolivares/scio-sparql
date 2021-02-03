import org.eclipse.rdf4j.rio.RDFFormat

class FilterTest extends SparqlPipelineTest {
  "SPARQL with FILTER" should "work correctly within an OPTIONAL" in {
    val query =
      """
        |PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
        |PREFIX  ns:  <http://example.org/ns#>
        |SELECT  ?title ?price
        |WHERE   { ?x dc:title ?title .
        |          OPTIONAL { ?x ns:price ?price . FILTER (?price < 30) }
        |        }
        |""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }

  it should "work correctly without OPTIONAL" in {
    val query =
      """
        |PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
        |PREFIX  ns:  <http://example.org/ns#>
        |SELECT  ?title ?price
        |WHERE   { ?x dc:title ?title .
        |          ?x ns:price ?price . FILTER (?price < 30)
        |        }
        |""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }

  it should "work correctly with EXISTS" in {
    val query =
      """
        |PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
        |PREFIX  ns:  <http://example.org/ns#>
        |SELECT  ?title ?price
        |WHERE   { ?x dc:title ?title .
        |          FILTER EXISTS { ?x ns:price ?price }
        |        }
        |""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }

  it should "work correctly with NOT EXISTS" in {
    val query =
      """
        |PREFIX  dc:  <http://purl.org/dc/elements/1.1/>
        |PREFIX  ns:  <http://example.org/ns#>
        |SELECT  ?title ?price
        |WHERE   { ?x dc:title ?title .
        |          FILTER NOT EXISTS { ?x ns:price ?price }
        |        }
        |""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }
}
