import org.eclipse.rdf4j.query.impl.EmptyBindingSet
import org.eclipse.rdf4j.rio.RDFFormat

class LimitsTest extends SparqlPipelineTest {
  "SPARQL with a slice" should "work correctly with a LIMIT" in {
    val query =
      """
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
        |PREFIX :     <http://example.org/book/> 
        |PREFIX ns:   <http://example.org/ns#> 
        |
        |SELECT *
        |{
        |   ?a ?b ?c
        |} LIMIT 2""".stripMargin
    testSparql(
      "values.ttl",
      RDFFormat.TURTLE,
      query,
      _ => Utils.EMPTY_BINDING_SET
    )
  }

  it should "work correctly with an OFFSET" in {
    val query =
      """
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
        |PREFIX :     <http://example.org/book/> 
        |PREFIX ns:   <http://example.org/ns#> 
        |
        |SELECT *
        |{
        |   ?a ?b ?c
        |} OFFSET 3""".stripMargin
    testSparql(
      "values.ttl",
      RDFFormat.TURTLE,
      query,
      _ => Utils.EMPTY_BINDING_SET
    )
  }

  it should "work correctly with an ORDER BY in a sliced context" in {
    val query =
      """
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
        |PREFIX :     <http://example.org/book/> 
        |PREFIX ns:   <http://example.org/ns#> 
        |
        |SELECT *
        |{
        |   ?a ?b ?c
        |} ORDER BY ?c
        |LIMIT 3""".stripMargin
    testSparql(
      "values.ttl",
      RDFFormat.TURTLE,
      query
    )
  }
}
