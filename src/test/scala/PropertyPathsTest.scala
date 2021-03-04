import org.eclipse.rdf4j.rio.RDFFormat

class PropertyPathsTest extends SparqlPipelineTest {
  "SPARQL queries with property paths" should "work if the path is finite" in {
    val query =
      """
        |PREFIX :   <http://example/>
        |SELECT * 
        |{  ?s :item/:price ?x . }""".stripMargin
    testSparql("property_paths.ttl", RDFFormat.TURTLE, query)
  }

  it should "work if the path is negated" in {
    val query =
      """
        |PREFIX :   <http://example/>
        |SELECT * 
        |{  ?s !:item ?x . }""".stripMargin
    testSparql("property_paths.ttl", RDFFormat.TURTLE, query)
  }

  it should "work if the path is with alternatives" in {
    val query =
      """
        |PREFIX :   <http://example/>
        |SELECT * 
        |{  ?s (:item | :price) ?x . }""".stripMargin
    testSparql("property_paths.ttl", RDFFormat.TURTLE, query)
  }
}
