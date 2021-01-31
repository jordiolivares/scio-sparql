import com.spotify.scio.ContextAndArgs
import es.jolivar.scio.sparql.TriplesReader._
import es.jolivar.scio.sparql.Interpreter._
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
    val query = args("query")
    val inFile = args("in")
    val outFile = args("out")
    sc.readTriples(inFile)
      .executeSparql(query)
      .map { x =>
        x.toList
          .sortBy(m => m._1)
          .map(m => m._2)
      }
      .saveAsTextFile(outFile)
    sc.run()
  }
}
