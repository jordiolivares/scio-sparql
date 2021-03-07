import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.vocabulary.XSD
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
import scala.jdk.OptionConverters._
import scala.util.{Try, Using}

object Utils {
  case class Value(value: String, dataType: IRI, language: Option[String])

  object Value {
    private val vf = SimpleValueFactory.getInstance()
    implicit val encoderIri: Encoder[IRI] =
      Encoder[String].contramap(_.stringValue())
    implicit val decoderIri: Decoder[IRI] =
      Decoder[String].emapTry(x => Try { vf.createIRI(x) })
    implicit val encoder: Encoder[Value] = deriveEncoder
    implicit val decoder: Decoder[Value] = deriveDecoder
  }

  type BindingSet = Map[String, Value]
  val EMPTY_BINDING_SET: BindingSet = Map()

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
          literal.getDatatype,
          literal.getLanguage.toScala
        )
      case _: IRI   => Value(stringValue, XSD.ANYURI, None)
      case _: BNode => Value(stringValue, XSD.ANYURI, None)
    }
  }

  implicit class RepositoryExt(val repo: Repository) extends AnyVal {
    def executeSparql(query: String): List[List[(String, rdf4jValue)]] = {
      Using.Manager { use =>
        val conn = use(repo.getConnection)
        val results = use(conn.prepareTupleQuery(query).evaluate())
        val bindings = results.asScala.toList
        bindings
          .map(
            _.asScala.toList.sortBy(_.getName).map(x => x.getName -> x.getValue)
          )
      }.get
    }
  }
}
