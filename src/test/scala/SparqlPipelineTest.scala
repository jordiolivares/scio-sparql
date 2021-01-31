import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import es.jolivar.scio.sparql.TriplesIO
import org.eclipse.rdf4j.rio.RDFFormat
import Utils._

trait SparqlPipelineTest extends PipelineSpec {
  def testSparql(
      resourceFile: String,
      rdfFormat: RDFFormat,
      query: String
  ): Unit = {
    val (dataset, repo) =
      Utils.getDatasetAndRepo(resourceFile, rdfFormat)
    val results = repo.executeSparql(query).map(_.toString())
    JobTest[SPARQLTestPipeline.type]
      .args(
        s"--query=$query",
        "--in=in.ttl",
        "--out=out.txt"
      )
      .input(TriplesIO("in.ttl"), dataset)
      .output(TextIO("out.txt")) { col =>
        col should containInAnyOrder(results)
        col.count should containValue(results.size.toLong)
      }
      .run()
  }
}
