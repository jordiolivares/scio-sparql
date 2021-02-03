import org.eclipse.rdf4j.rio.RDFFormat

class ValuesTest extends SparqlPipelineTest {
  "SPARQL with VALUES" should "work correctly for case #1" in {
    val query =
      """
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
        |PREFIX :     <http://example.org/book/> 
        |PREFIX ns:   <http://example.org/ns#> 
        |
        |SELECT ?book ?title ?price
        |{
        |   VALUES ?book { :book1 :book3 }
        |   ?book dc:title ?title ;
        |         ns:price ?price .
        |}""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }

  it should "work correctly for case #2 with UNDEF" in {
    val query =
      """
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
        |PREFIX :     <http://example.org/book/> 
        |PREFIX ns:   <http://example.org/ns#> 
        |
        |SELECT ?book ?title ?price
        |{
        |   ?book dc:title ?title ;
        |         ns:price ?price .
        |   VALUES (?book ?title)
        |   { (UNDEF "SPARQL Tutorial")
        |     (:book2 UNDEF)
        |   }
        |}""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }

  it should "work correctly when doing a BIND" in {
    val query =
      """
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/> 
        |PREFIX :     <http://example.org/book/> 
        |PREFIX ns:   <http://example.org/ns#> 
        |
        |SELECT ?book ?title ?price
        |{
        |   BIND(:book1 as ?book)
        |   ?book dc:title ?title ;
        |         ns:price ?hiddenPrice .
        |   BIND((?hiddenPrice * ?hiddenPrice) as ?price)
        |}""".stripMargin
    testSparql("values.ttl", RDFFormat.TURTLE, query)
  }
}
