import org.eclipse.rdf4j.model.{
  BNode,
  IRI,
  Literal,
  Resource,
  Statement,
  Value => rdf4jValue
}
import org.eclipse.rdf4j.repository.Repository
import org.eclipse.rdf4j.repository.sail.SailRepository
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.sail.memory.MemoryStore

import scala.jdk.CollectionConverters._
import scala.util.Using

object Utils {
  case class Value(value: String, dataType: String)
  type BindingSet = Map[String, Value]

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

  def rdf4jValue2OurValue(value: rdf4jValue): Value = {
    val stringValue = value.stringValue()
    value match {
      case literal: Literal =>
        Value(
          stringValue,
          literal.getDatatype.toString + literal.getLanguage.orElse("")
        )
      case _: IRI   => Value(stringValue, stringValue)
      case _: BNode => Value(stringValue, stringValue)
    }
  }

  implicit class RepositoryExt(val repo: Repository) extends AnyVal {
    def executeSparql(query: String): List[List[(String, rdf4jValue)]] = {
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
