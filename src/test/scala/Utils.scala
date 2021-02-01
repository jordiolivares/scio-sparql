import org.eclipse.rdf4j.model.{Statement, Value}
import org.eclipse.rdf4j.repository.Repository
import org.eclipse.rdf4j.repository.sail.SailRepository
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.sail.memory.MemoryStore

import scala.jdk.CollectionConverters._
import scala.util.Using

object Utils {
  def getDatasetAndRepo(
      resourceFile: String,
      rdfFormat: RDFFormat
  ): (List[Statement], SailRepository) = {
    val dataset =
      SPARQLTestPipeline.parseStatements(resourceFile, rdfFormat)

    val repo: SailRepository = {
      val repo = new SailRepository(new MemoryStore)
      val conn = repo.getConnection
      conn.add(dataset.asJava)
      conn.commit()
      conn.close()
      repo
    }
    (dataset, repo)
  }

  implicit class RepositoryExt(val repo: Repository) extends AnyVal {
    def executeSparql(query: String): List[List[(String, Value)]] = {
      Using.Manager { use =>
        val conn = use(repo.getConnection)
        val results = use(conn.prepareTupleQuery(query).evaluate())
        results.asScala
          .map(
            _.asScala.toList.sortBy(_.getName).map(x => x.getName -> x.getValue)
          )
          .toList
      }.get
    }
  }
}
