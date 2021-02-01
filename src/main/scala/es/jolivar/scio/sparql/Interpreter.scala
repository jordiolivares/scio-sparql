package es.jolivar.scio.sparql

import com.spotify.scio.values.SCollection
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.{IRI, Literal, Resource, Statement, Value}
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp
import org.eclipse.rdf4j.query.algebra._
import org.eclipse.rdf4j.query.parser.{ParsedTupleQuery, QueryParserUtil}
import org.eclipse.rdf4j.query.{BindingSet, QueryLanguage}

import ValueEvaluators._

import scala.jdk.CollectionConverters._

object Interpreter {
  type ResultSet = Map[String, Value]
  private val EMPTY_RESULT_SET: ResultSet = Map()
  private type Bindings = List[String]

  private val vf = SimpleValueFactory.getInstance()

  implicit class ResultSetExt(val resultSet: ResultSet) extends AnyVal {
    def evaluateValueExpr(valueExpr: ValueExpr): (String, Option[Value]) = {
      valueExpr match {
        case varExpr: Var if varExpr.hasValue =>
          varExpr.getName -> Some(varExpr.getValue)
        case varExpr: Var => varExpr.getName -> resultSet.get(varExpr.getName)
        case sum: Sum     => evaluateValueExpr(sum.getArg)
        case count: Count => evaluateValueExpr(count.getArg)
      }
    }
  }

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
      if (v.hasValue) {
        v.getValue == value
      } else {
        true
      }
    }
  }

  implicit class StatementPatternExt(val statementPattern: StatementPattern)
      extends AnyVal {
    def matches(stmt: Statement): Boolean = {
      val graph = statementPattern.getContextVar
      val subject = statementPattern.getSubjectVar
      val predicate = statementPattern.getPredicateVar
      val `object` = statementPattern.getObjectVar
      val graphMatches = {
        val graphIsNull = (stmt.getContext == null && graph == null)
        val graphIsEqual = (graph != null) && graph.matches(stmt.getContext)
        graphIsNull || graphIsEqual
      }
      val tripleMatches = {
        val subjectMatches = subject.matches(stmt.getSubject)
        val predicateMatches = predicate.matches(
          stmt.getPredicate
        )
        val objectMatches = `object`.matches(stmt.getObject)
        subjectMatches && predicateMatches && objectMatches
      }
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

  implicit class BindingsExt(val bindings: Bindings) extends AnyVal {
    def getBindingsOf(resultSet: ResultSet): List[Option[Value]] = {
      bindings.map(k => resultSet.get(k))
    }

    def getBindingsForKeying(resultSet: ResultSet): List[Option[String]] = {
      getBindingsOf(resultSet).map(_.map(_.toString))
    }
  }

  implicit class BindingSetExt(val bindingSet: BindingSet) extends AnyVal {
    def toResultSet: ResultSet = {
      bindingSet.asScala.map { binding =>
        binding.getName -> binding.getValue
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
    ): SCollection[ResultSet] = {
      val parsedQuery = parseSparql(query)
      col.transform("Performing SPARQL Query") { c =>
        processOperation(c)(parsedQuery.getTupleExpr)
      }
    }
  }

  def processOperation(fullDataset: SCollection[Statement])(
      tupleExpr: TupleExpr
  ): SCollection[ResultSet] = {
    val sc = fullDataset.context
    val results = tupleExpr match {
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
          val keyedLeft = leftDataset.keyBy(commonBindings.getBindingsForKeying)
          val keyedRight =
            rightDataset.keyBy(commonBindings.getBindingsForKeying)
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
          val keyedLeft = leftDataset.keyBy(commonBindings.getBindingsForKeying)
          val keyedRight =
            rightDataset.keyBy(commonBindings.getBindingsForKeying)
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
        results.distinctBy(bindings.getBindingsForKeying)
      case reduced: Reduced =>
        val results = processOperation(fullDataset)(reduced.getArg)
        val bindings = reduced.getBindingNames.asScala.toList
        results.distinctBy(bindings.getBindingsForKeying)
      case group: Group =>
        val results = processOperation(fullDataset)(group.getArg)
        val bindingsToGroupBy = group.getGroupBindingNames.asScala.toList
        val keyedResults = results.keyBy(bindingsToGroupBy.getBindingsForKeying)
        val groupElems = group.getGroupElements.asScala.map { groupElem =>
          val bindingName = groupElem.getName
          val resultsPerKey = groupElem.getOperator match {
            case count: Count =>
              def reductionFunction(
                  left: (ResultSet, Int),
                  right: (ResultSet, Int)
              ) = {
                left._1 -> (left._2 + right._2)
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(count.getArg)
                  )
                  .collect {
                    case (key, resultSet -> (_ -> Some(value))) =>
                      key -> (resultSet -> value)
                  }
              val countedPerKey = if (!count.isDistinct) {
                evaluatedExpr
                  .mapValues {
                    case (resultSet, _) => resultSet -> 1
                  }
                  .reduceByKey(reductionFunction)
              } else {
                evaluatedExpr
                  .map {
                    case (k, (resultSet, value)) =>
                      (k, value.toString) -> (resultSet -> 1)
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((k, _), pair) => k -> pair
                  }
                  .reduceByKey(reductionFunction)
              }
              countedPerKey.mapValues {
                case (resultSet, aggregatedCount) =>
                  resultSet -> Map(
                    bindingName -> vf
                      .createLiteral(aggregatedCount)
                      .asInstanceOf[Value]
                  )
              }
            case sum: Sum =>
              def reductionFunction(
                  left: (ResultSet, Literal),
                  right: (ResultSet, Literal)
              ) = {
                left._1 -> (left._2 + right._2)
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(sum.getArg)
                  )
                  .collect {
                    case (key, resultSet -> (_ -> Some(literal))) =>
                      key -> (resultSet -> literal.asInstanceOf[Literal])
                  }
              val summedPerKey = if (!sum.isDistinct) {
                evaluatedExpr.reduceByKey(reductionFunction)
              } else {
                evaluatedExpr
                  .map {
                    case (key, pair @ (_, lit)) => (key, lit.toString) -> pair
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((key, _), pair) => key -> pair
                  }
                  .reduceByKey(reductionFunction)
              }
              summedPerKey.mapValues {
                case (resultSet, aggregatedLiteral) =>
                  resultSet -> Map(
                    bindingName -> aggregatedLiteral.asInstanceOf[Value]
                  )
              }
          }
          resultsPerKey
        }
        groupElems
          .reduce { (map1, map2) =>
            map1.join(map2).mapValues {
              case ((x, aggregations1), (_, aggregations2)) =>
                x -> (aggregations1 ++ aggregations2)
            }
          }
          .values
          .map {
            case (resultSet, value) =>
              val groupingBindings =
                resultSet.filter(x => bindingsToGroupBy.contains(x._1))
              groupingBindings ++ value
          }
      case bindingSetAssignment: BindingSetAssignment =>
        val bindings =
          bindingSetAssignment.getBindingSets.asScala.map(_.toResultSet)
        sc.parallelize(bindings)
      case extension: Extension =>
        val results = processOperation(fullDataset)(extension.getArg)
        val extraValues = extension.getElements.asScala.map(_.getExpr)
        results.map { resultSet =>
          val extensionValues =
            extraValues.map(resultSet.evaluateValueExpr).collect {
              case k -> Some(v) => k -> v
            }
          resultSet ++ extensionValues
        }
    }
    results.tap { resultSet =>
      tupleExpr -> resultSet
    }
  }
}
