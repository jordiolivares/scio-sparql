import org.eclipse.rdf4j.rio.RDFFormat

class AggregatesTest extends SparqlPipelineTest {
  "SPARQL with GROUP BY" should "work with a SUM" in {
    val query =
      """
        |PREFIX : <http://books.example/>
        |SELECT (SUM(?lprice) AS ?totalPrice)
        |WHERE {
        |  ?org :affiliates ?auth .
        |  ?auth :writesBook ?book .
        |  ?book :price ?lprice .
        |}
        |GROUP BY ?org
        |HAVING (SUM(?lprice) > 10)""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }
}
