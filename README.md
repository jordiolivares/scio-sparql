# Scio + SPARQL

The scio-sparql project is a [Scio](https://github.com/spotify/scio) extension
that lets you use [SPARQL](https://en.wikipedia.org/wiki/SPARQL) on top of a
collection of RDF triples.

# Features
* _Mostly_ SPARQL 1.1 compliant
* [rdf4j](https://rdf4j.org/) based
* RDF/Turtle/Trig/etc. parsers for statements

# Quick Start

```scala
import com.spotify.scio.values.SCollection
import es.jolivar.scio.sparql.Interpreter.SCollectionStatements
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.query.BindingSet

val statements: SCollection[Statement] = ???
val bindingSets: SCollection[BindingSet] = statements.executeSparql(
  """
    |PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    |SELECT ?name 
    |       ?email
    |WHERE
    |  {
    |    ?person  a          foaf:Person .
    |    ?person  foaf:name  ?name .
    |    ?person  foaf:mbox  ?email .
    |  }""".stripMargin
)
```

# SPARQL Limitations
Since Apache Beam doesn't have a concept of ordering there's no support for `ORDER BY` nor will this ever support it.
In fact, this is a no-op in the implementation.

`MINUS` and inner `FILTER`s are not currently supported due to the fact of having to process statement by statement
individually.

Property paths that use are supported as long as they are finite. In particular, the following quantity modifiers
aren't supported:
* `iri*` ZeroOrMorePath
* `iri+` OneOrMorePath

An additional limitation due to the nature of the Apache Beam model is that if there's a duplicate statement in the
original SCollection it will surface through to the end if the query matches it. To avoid this it would mean making a
`SCollection[Statement].distinct` shuffle, which is potentially very expensive.