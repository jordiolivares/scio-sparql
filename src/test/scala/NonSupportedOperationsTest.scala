import org.eclipse.rdf4j.rio.RDFFormat

class NonSupportedOperationsTest extends SparqlPipelineTest {
  "SPARQL with ORDER BY and no slice" should "ignore a ASC" in {
    val query =
      """
        |PREFIX : <http://books.example/>
        |SELECT ?org (SUM(?lprice) AS ?totalPrice)
        |WHERE {
        |  ?org :affiliates ?auth .
        |  ?auth :writesBook ?book .
        |  ?book :price ?lprice .
        |}
        |GROUP BY ?org
        |ORDER BY ASC(?totalPrice)""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "ignore a DESC" in {
    val query =
      """
        |PREFIX : <http://books.example/>
        |SELECT ?org (SUM(?lprice) AS ?totalPrice)
        |WHERE {
        |  ?org :affiliates ?auth .
        |  ?auth :writesBook ?book .
        |  ?book :price ?lprice .
        |}
        |GROUP BY ?org
        |ORDER BY DESC(?totalPrice)""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }
}
