import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import es.jolivar.scio.sparql.TriplesIO
import org.eclipse.rdf4j.rio.RDFFormat
import io.circe.parser.decode
import io.circe.generic.auto._
import Utils._

trait SparqlPipelineTest extends PipelineSpec {

  def testSparql(
      resourceFile: String,
      rdfFormat: RDFFormat,
      query: String,
      applyFunctionResults: Utils.BindingSet => Utils.BindingSet = identity
  ): Unit = {
    val (dataset, repo) =
      Utils.getDatasetAndRepo(resourceFile, rdfFormat)
    val results = repo
      .executeSparql(query)
      .map { resultSet =>
        resultSet.toMap.map {
          case key -> value =>
            key -> rdf4jValue2OurValue(value)
        }
      }
      .map(applyFunctionResults)
    JobTest[SPARQLTestPipeline.type]
      .args(
        s"--query=$query",
        "--in=in.ttl",
        "--out=out.txt"
      )
      .input(TriplesIO("in.ttl"), dataset)
      .output(TextIO("out.txt")) { col =>
        col
          .map(x => decode[Utils.BindingSet](x).getOrElse(Map()))
          .map(applyFunctionResults) should containInAnyOrder(results)
        col.count should containValue(results.size.toLong)
      }
      .run()
  }
}
