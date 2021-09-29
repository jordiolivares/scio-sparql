import com.spotify.scio.ContextAndArgs
import es.jolivar.scio.sparql.TriplesReader._
import es.jolivar.scio.sparql.Interpreter._
import io.circe.syntax.EncoderOps
import io.circe.generic.auto._
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import scala.jdk.CollectionConverters._

object SPARQLTestPipeline {

  def parseStatements(
      resourceFileName: String,
      rdfFormat: RDFFormat
  ): List[Statement] = {
    val is = getClass.getResourceAsStream(resourceFileName)
    Rio.parse(is, rdfFormat).iterator().asScala.toList
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val query =
      try {
        args("query")
      } catch {
        case _: Throwable => args.list("query").mkString(",")
      }
    val inFile = args("in")
    val outFile = args("out")
    val triples = sc
      .readTriples(inFile)
      .executeSparql(query)
    val triplesSideInput = triples.asListSideInput
    val dummyScollection = sc.parallelize(Seq(1))
    dummyScollection
      .withSideInputs(triplesSideInput)
      .flatMap { case (_, ctx) =>
        val triples = ctx(triplesSideInput)
        triples.map { x =>
          x.iterator()
            .asScala
            .toList
            .map(m => (m.getName, Utils.rdf4jValue2OurValue(m.getValue)))
            .toMap
            .asJson
            .noSpaces
        }
      }
      .toSCollection
      .saveAsTextFile(outFile)
    sc.run()
  }
}
