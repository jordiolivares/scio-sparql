import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import es.jolivar.scio.sparql.TriplesIO
import org.eclipse.rdf4j.rio.RDFFormat
import io.circe.parser.decode
import io.circe.generic.auto._
import Utils._
import cats.Eq
import org.apache.beam.sdk.testing.PAssert
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.vocabulary.XSD

trait SparqlPipelineTest extends PipelineSpec {

  def testSparql(
      resourceFile: String,
      rdfFormat: RDFFormat,
      query: String,
      applyFunctionResults: Utils.BindingSet => Utils.BindingSet = identity
  ): Unit = {
    val (dataset, repo) =
      Utils.getDatasetAndRepo(resourceFile, rdfFormat)
    val tripleResults = repo
      .executeSparql(query)
    val results = tripleResults
      .map { resultSet =>
        resultSet.toMap.map { case key -> value =>
          key -> rdf4jValue2OurValue(value)
        }
      }
      .map(applyFunctionResults)
    implicit val eqValue: Eq[Value] = new Eq[Value] {
      override def eqv(x: Value, y: Value): Boolean = {
        (x, y) match {
          case (Value(xVal, XSD.DECIMAL, _), Value(yVal, XSD.DECIMAL, _)) =>
            (BigDecimal(xVal) - BigDecimal(yVal)).abs < BigDecimal(0.0001)
          case _ => x == y
        }
      }
    }

    implicit val eqBindingSet: Eq[BindingSet] =
      cats.implicits.catsKernelStdEqForMap
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
        col should haveSize(results.size)
      }
      .run()
  }
}
