import org.eclipse.rdf4j.rio.RDFFormat

class GraphsTest extends SparqlPipelineTest {
  "SPARQL with GRAPH" should "work correctly with mixed default and named graphs" in {
    val query =
      """
        |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/>
        |
        |SELECT ?name ?mbox ?date
        |WHERE
        |  {  ?g dc:publisher ?name ;
        |        dc:date ?date .
        |    GRAPH ?g
        |      { ?person foaf:name ?name ; foaf:mbox ?mbox }
        |  }""".stripMargin
    testSparql("graphs.trig", RDFFormat.TRIG, query)
  }

  // FIXME: Re-enable the test when rdf4j fixes an issue on how they parse ZeroOrOne Property Paths
  //  apparently they are returning one less item than us and i've checked with another implementation
  //  that returns the exact same result set as I do. This means that RDF4J is in the wrong here, so this
  //  test will always fail
  ignore should "work correctly with ZeroOrOne property paths" in {
    val query =
      """
        |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/>
        |
        |SELECT ?name ?mbox ?date
        |WHERE
        |  {  ?g dc:publisher ?name ;
        |        dc:date ?date .
        |    GRAPH ?g
        |      { ?person foaf:mbox/foaf:name? ?mbox }
        |  }""".stripMargin
    testSparql("graphs.trig", RDFFormat.TRIG, query)
  }

  it should "work correctly with values and named graphs" in {
    val query =
      """
        |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        |PREFIX dc:   <http://purl.org/dc/elements/1.1/>
        |prefix g:  <tag:example.org,2005-06-06:>
        |
        |SELECT ?item ?otherItem ?name ?type
        |WHERE
        |  {
        |    VALUES ?type { g:class g:otherClass g:noClass }
        |    GRAPH <tag:graph3> {
        |       ?item g:in ?otherItem .
        |    }
        |    GRAPH <tag:graph4> {
        |        ?otherItem foaf:name ?name ;
        |                     a ?type .
        |    }
        |  }""".stripMargin
    testSparql("graphs.trig", RDFFormat.TRIG, query)
  }
}
