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

  it should "work with a MIN" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?g (MIN(?p) AS ?min)
        |WHERE {
        |  ?g :p ?p .
        |}
        |GROUP BY ?g""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with a complex MIN" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?x (MIN(?y) * 2 AS ?min)
        |WHERE {
        |  ?x :p ?y .
        |} GROUP BY ?x""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with SUM" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?x (SUM(?y) * 2 AS ?sum)
        |WHERE {
        |  ?x :p ?y .
        |} GROUP BY ?x""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with MAX" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?x (MAX(?y) * 2 AS ?max)
        |WHERE {
        |  ?x :p ?y .
        |} GROUP BY ?x""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with GROUP_CONCAT" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?x (GROUP_CONCAT(?y; separator=";") AS ?concat)
        |WHERE {
        |  ?x :p ?y .
        |} GROUP BY ?x""".stripMargin
    testSparql(
      "group_by.ttl",
      RDFFormat.TURTLE,
      query,
      resultSet => {
        resultSet.map {
          case k -> value =>
            k -> value.copy(value = value.value.split(";").sorted.mkString(";"))
        }
      }
    )
  }

  it should "work with AVG" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?x (AVG(?y) AS ?avg)
        |WHERE {
        |  ?x :p ?y .
        |} GROUP BY ?x""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }

  it should "work with multiple aggregate variables" in {
    val query =
      """
        |PREFIX : <http://example.com/data/#>
        |SELECT ?g (AVG(?p) AS ?avg) ((MIN(?p) + MAX(?p)) / 2 AS ?c)
        |WHERE {
        |  ?g :p ?p .
        |}
        |GROUP BY ?g""".stripMargin
    testSparql("group_by.ttl", RDFFormat.TURTLE, query)
  }
}
