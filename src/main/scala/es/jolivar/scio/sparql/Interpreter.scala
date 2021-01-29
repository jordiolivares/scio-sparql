package es.jolivar.scio.sparql

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.eclipse.rdf4j.query.{BindingSet, QueryLanguage}
import org.eclipse.rdf4j.query.algebra.{
  BindingSetAssignment,
  Distinct,
  Exists,
  Extension,
  Filter,
  Group,
  Join,
  LeftJoin,
  Projection,
  Reduced,
  StatementPattern,
  TupleExpr,
  Union,
  Var
}
import org.eclipse.rdf4j.query.parser.{ParsedTupleQuery, QueryParserUtil}
import org.eclipse.rdf4j.model.{IRI, Resource, Statement, Value}

import scala.jdk.CollectionConverters._

object Interpreter {
  type ResultSet = Map[String, Option[Value]]
  private val EMPTY_RESULT_SET: ResultSet = Map()
  private type Bindings = List[String]

  implicit class VarExt(val v: Var) extends AnyVal {
    def matches(resource: Resource): Boolean = {
      if (v.hasValue) {
        val value = v.getValue
        value.isInstanceOf[Resource] && value.asInstanceOf[Resource] == resource
      } else {
        true
      }
    }

    def matches(iri: IRI): Boolean = {
      if (v.hasValue) {
        val value = v.getValue
        value.isInstanceOf[IRI] && value.asInstanceOf[IRI] == iri
      } else {
        true
      }
    }

    def matches(value: Value): Boolean = {
      v.getValue == value
    }
  }

  implicit class StatementPatternExt(val statementPattern: StatementPattern)
      extends AnyVal {
    def matches(stmt: Statement): Boolean = {
      val graph = statementPattern.getContextVar
      val subject = statementPattern.getSubjectVar
      val predicate = statementPattern.getPredicateVar
      val `object` = statementPattern.getObjectVar
      val graphMatches = graph != null && graph.matches(stmt.getContext)
      val tripleMatches = subject.matches(stmt.getSubject) && predicate.matches(
        stmt.getPredicate
      ) && `object`.matches(stmt.getObject)
      graphMatches && tripleMatches
    }

    def convertToResultSet(stmt: Statement): ResultSet = {
      var resultSet: ResultSet = Map()
      if (!statementPattern.getSubjectVar.hasValue) {
        resultSet += statementPattern.getSubjectVar.getName -> Option(
          stmt.getSubject
        )
      }
      if (!statementPattern.getPredicateVar.hasValue) {
        resultSet += statementPattern.getPredicateVar.getName -> Option(
          stmt.getPredicate
        )
      }
      if (!statementPattern.getObjectVar.hasValue) {
        resultSet += statementPattern.getObjectVar.getName -> Option(
          stmt.getObject
        )
      }
      if (
        statementPattern.getContextVar != null && !statementPattern.getContextVar.hasValue
      ) {
        resultSet += statementPattern.getContextVar.getName -> Option(
          stmt.getContext
        )
      }
      resultSet
    }
  }

  implicit class BindingsExt(val bindings: Bindings) extends AnyVal {
    def getBindingsOf(resultSet: ResultSet): List[Option[Value]] = {
      bindings.map(k => resultSet(k))
    }
  }

  implicit class BindingSetExt(val bindingSet: BindingSet) extends AnyVal {
    def toResultSet: ResultSet = {
      bindingSet.asScala.map { binding =>
        binding.getName -> Some(binding.getValue)
      }.toMap
    }
  }

  def parseSparql(query: String): ParsedTupleQuery = {
    QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null)
  }

  implicit class SCollectionStatements(val col: SCollection[Statement])
      extends AnyVal {
    def executeSparql(
        query: String
    )(implicit sc: ScioContext): SCollection[ResultSet] = {
      val parsedQuery = parseSparql(query)
      processOperation(col)(parsedQuery.getTupleExpr)
    }
  }

  def processOperation(fullDataset: SCollection[Statement])(
      tupleExpr: TupleExpr
  )(implicit sc: ScioContext): SCollection[ResultSet] = {
    tupleExpr match {
      case statementPattern: StatementPattern =>
        fullDataset
          .filter(statementPattern.matches)
          .map(statementPattern.convertToResultSet)
      case join: Join =>
        val leftDataset = processOperation(fullDataset)(join.getLeftArg)
        val rightDataset = processOperation(fullDataset)(join.getRightArg)
        val leftBindings = join.getLeftArg.getBindingNames.asScala
        val rightBindings = join.getRightArg.getBindingNames.asScala
        val commonBindings =
          (leftBindings & rightBindings).toList // Deterministic order when iterating
        if (commonBindings.isEmpty) {
          leftDataset.cross(rightDataset).map {
            case (left, right) =>
              left ++ right
          }
        } else {
          val keyedLeft = leftDataset.keyBy(commonBindings.getBindingsOf)
          val keyedRight = rightDataset.keyBy(commonBindings.getBindingsOf)
          keyedLeft.join(keyedRight).values.map {
            case (left, right) =>
              left ++ right
          }
        }
      case leftJoin: LeftJoin =>
        val leftDataset = processOperation(fullDataset)(leftJoin.getLeftArg)
        val rightDataset = processOperation(fullDataset)(leftJoin.getRightArg)
        val leftBindings = leftJoin.getLeftArg.getBindingNames.asScala
        val rightBindings = leftJoin.getRightArg.getBindingNames.asScala
        val commonBindings =
          (leftBindings & rightBindings).toList // Deterministic order when iterating
        if (commonBindings.isEmpty) {
          leftDataset.cross(rightDataset).map {
            case (left, right) =>
              left ++ right
          }
        } else {
          val keyedLeft = leftDataset.keyBy(commonBindings.getBindingsOf)
          val keyedRight = rightDataset.keyBy(commonBindings.getBindingsOf)
          keyedLeft.leftOuterJoin(keyedRight).values.map {
            case (left, right) =>
              left ++ right.getOrElse(EMPTY_RESULT_SET)
          }
        }
      case projection: Projection =>
        val results = processOperation(fullDataset)(projection.getArg)
        val keys = projection.getBindingNames.asScala.toSet
        val projectionElements =
          projection.getProjectionElemList.getElements.asScala
        projectionElements.map(_.getSourceName)
        results.map { resultSet =>
          resultSet.filter {
            case k -> _ => keys.contains(k)
          }
        }
      case union: Union =>
        val leftDataset = processOperation(fullDataset)(union.getLeftArg)
        val rightDataset = processOperation(fullDataset)(union.getRightArg)
        leftDataset.union(rightDataset)
      // TODO: Implement Filter
      case distinct: Distinct =>
        val results = processOperation(fullDataset)(distinct.getArg)
        val bindings = distinct.getBindingNames.asScala.toList
        results.distinctBy(bindings.getBindingsOf)
      case reduced: Reduced =>
        val results = processOperation(fullDataset)(reduced.getArg)
        val bindings = reduced.getBindingNames.asScala.toList
        results.distinctBy(bindings.getBindingsOf)
      // TODO: Groupings
      case bindingSetAssignment: BindingSetAssignment =>
        val bindings =
          bindingSetAssignment.getBindingSets.asScala.map(_.toResultSet)
        sc.parallelize(bindings)
      case extension: Extension =>
        val results = processOperation(fullDataset)(extension.getArg)
        val extraValues = extension.getElements.asScala
          .map(_.getExpr)
          .foldLeft(EMPTY_RESULT_SET) {
            case (acc, varDecl: Var) =>
              acc + (varDecl.getName -> Option(varDecl.getValue))
          }
        results.map { resultSet =>
          resultSet ++ extraValues
        }
    }
  }
}
