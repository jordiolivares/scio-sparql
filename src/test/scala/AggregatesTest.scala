import org.eclipse.rdf4j.rio.RDFFormat

class AggregatesTest extends SparqlPipelineTest {
  "SPARQL with GROUP BY" should "work with a SUM" in {
    val query =
      """
        |PREFIX : <http://books.example/>
        |SELECT ?org (SUM(?lprice) AS ?totalPrice)
        |WHERE {
        |  ?org :affiliates ?auth .
        |  ?auth :writesBook ?book .
        |  ?book :price ?lprice .
        |}
        |GROUP BY ?org""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with a COUNT" in {
    val query =
      """
        |PREFIX : <http://books.example/>
        |SELECT ?auth (COUNT(?book) AS ?numBooks)
        |WHERE {
        |  ?auth :writesBook ?book .
        |} GROUP BY ?auth""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }
}
