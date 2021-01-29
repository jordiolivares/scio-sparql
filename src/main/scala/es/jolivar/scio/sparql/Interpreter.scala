package es.jolivar.scio.sparql

import com.spotify.scio.values.SCollection
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.query.algebra.{Join, StatementPattern, TupleExpr, Var}
import org.eclipse.rdf4j.query.parser.{ParsedTupleQuery, QueryParserUtil}
import org.eclipse.rdf4j.model.{IRI, Resource, Statement, Value}

object Interpreter {
  type ResultSet = Map[String, Value]

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
        resultSet += statementPattern.getSubjectVar.getName -> stmt.getSubject
      }
      if (!statementPattern.getPredicateVar.hasValue) {
        resultSet += statementPattern.getPredicateVar.getName -> stmt.getPredicate
      }
      if (!statementPattern.getObjectVar.hasValue) {
        resultSet += statementPattern.getObjectVar.getName -> stmt.getObject
      }
      if (
        statementPattern.getContextVar != null && !statementPattern.getContextVar.hasValue
      ) {
        resultSet += statementPattern.getContextVar.getName -> stmt.getContext
      }
      resultSet
    }
  }

  def parseSparql(query: String): ParsedTupleQuery = {
    QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null)
  }

  def processOperation(fullDataset: SCollection[Statement])(
      tupleExpr: TupleExpr
  )(values: SCollection[ResultSet]): SCollection[ResultSet] = {
    tupleExpr match {
      case statementPattern: StatementPattern =>
        fullDataset
          .filter(statementPattern.matches)
          .map(statementPattern.convertToResultSet)
      case join: Join =>
        val leftDataset = processOperation(fullDataset)(join.getLeftArg)(values)
    }
  }
}
