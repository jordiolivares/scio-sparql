# Scio + SPARQL = Distributed offline graph database

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
bindingSets.map { bindingSet =>
  val name = bindingSet.getValue("name").stringValue()
  // Do something with the bindings
}
```

# SPARQL Limitations
Since Apache Beam doesn't have a concept of ordering there's no support for `ORDER BY`.
In fact, this is a no-op in the implementation. What *is* supported though it's using `ORDER BY` in a sliced context.
That is to say, that the following query will return the 3 largest elements after skiping the first 5 largest ones:

```sparql
SELECT ?a ?b ?c
WHERE {
  ?a ?b ?c
}
ORDER BY ?c
OFFSET 5
LIMIT 3 
```

A caveat about it though, that since Apache Beam offers no ordering guarantees, the results will not appear in order
but are guaranteed to be the elements of the correct result set.

`MINUS` and inner `FILTER`s are not currently supported due to the fact of having to process statement by statement
individually.

Property paths are supported as long as they are finite. In particular, the following quantity modifiers
aren't supported:
* `iri*` ZeroOrMorePath
* `iri+` OneOrMorePath

An additional limitation due to the nature of the Apache Beam model is that if there's a duplicate statement in the
original SCollection it will surface through to the end if the query matches it. To avoid this it would mean making a
`SCollection[Statement].distinct` shuffle at the beginning, which is potentially *very* expensive.

# How it works

At dataflow building time the program parses the SPARQL query and converts the query into its equivalent algebra
operations. It then proceeds to build the operations based on interpreting the algebra into its equivalence in Scio
operations. This in turn statically compiles the dataflow for execution, meaning the overhead is equivalent to writing
out the operations by hand.

**One very important caveat** to notice is that SPARQL is very, *very* prone to using JOINS. This will translate to very
large amounts of shuffling, but that's the price to pay for running this process in a distributed fashion.

In economic terms this means that some queries might have a larger shuffle cost than a compute cost. In particular
this is true of Cloud Dataflow using its Dataflow Shuffle service.

One improvement that could be made would be to 