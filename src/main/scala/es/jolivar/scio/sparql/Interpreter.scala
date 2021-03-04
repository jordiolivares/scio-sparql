package es.jolivar.scio.sparql

import com.spotify.scio.values.SCollection
import es.jolivar.scio.sparql.ValueEvaluators._
import org.eclipse.rdf4j.common.iteration.CloseableIteration
import org.eclipse.rdf4j.model._
import org.eclipse.rdf4j.model.impl.SimpleValueFactory
import org.eclipse.rdf4j.model.vocabulary.XSD
import org.eclipse.rdf4j.query.algebra._
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource
import org.eclipse.rdf4j.query.algebra.evaluation.federation.{
  FederatedService,
  FederatedServiceResolver
}
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy
import org.eclipse.rdf4j.query.impl.{EmptyBindingSet, MapBindingSet}
import org.eclipse.rdf4j.query.parser.{ParsedTupleQuery, QueryParserUtil}
import org.eclipse.rdf4j.query.{
  Binding,
  BindingSet,
  QueryEvaluationException,
  QueryLanguage
}

import scala.jdk.CollectionConverters._

object Interpreter {
  type ResultSet = BindingSet
  private val EMPTY_RESULT_SET: ResultSet = EmptyBindingSet.getInstance()
  private type Bindings = List[String]

  private val vf = SimpleValueFactory.getInstance()
  private val LITERAL_ONE = vf.createLiteral(1L)

  private val evaluator = {
    val dummyResolver = new FederatedServiceResolver {
      override def getService(serviceUrl: String): FederatedService = ???
    }
    val emptyTripleSource = new TripleSource {
      override def getStatements(
          subj: Resource,
          pred: IRI,
          obj: Value,
          contexts: Resource*
      ): CloseableIteration[_ <: Statement, QueryEvaluationException] = ???

      override def getValueFactory: ValueFactory = vf
    }
    new StrictEvaluationStrategy(emptyTripleSource, dummyResolver)
  }

  implicit class ResultSetExt(val resultSet: ResultSet) extends AnyVal {
    def evaluateValueExpr(valueExpr: ValueExpr): Option[Value] = {
      valueExpr match {
        case operator: AbstractAggregateOperator =>
          evaluateValueExpr(operator.getArg)
        case _ =>
          try {
            Some(evaluator.evaluate(valueExpr, resultSet))
          } catch {
            case ex: Throwable => None
          }
      }
    }

    def ++(other: ResultSet): ResultSet = {
      val result = new MapBindingSet(other.size() + resultSet.size())
      resultSet
        .iterator()
        .forEachRemaining(x => {
          result.addBinding(x)
        })
      other
        .iterator()
        .forEachRemaining(x => {
          result.addBinding(x)
        })
      result
    }

    def filter(f: (String, Value) => Boolean): BindingSet = {
      val filteredResultSet = new MapBindingSet()
      resultSet
        .iterator()
        .asScala
        .filter(x => f(x.getName, x.getValue))
        .foreach(x => filteredResultSet.addBinding(x))
      filteredResultSet
    }

    def nonNullBindings: Iterable[Binding] = resultSet.asScala

    def canBeJoined(
        bindingNames: Iterable[String],
        otherBinding: BindingSet
    ): Boolean = {
      bindingNames.forall { bindingName =>
        otherBinding.getValue(bindingName) == resultSet.getValue(bindingName)
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
      val resultSet = new MapBindingSet()
      if (!statementPattern.getSubjectVar.hasValue) {
        resultSet.addBinding(
          statementPattern.getSubjectVar.getName,
          stmt.getSubject
        )
      }
      if (!statementPattern.getPredicateVar.hasValue) {
        resultSet.addBinding(
          statementPattern.getPredicateVar.getName,
          stmt.getPredicate
        )
      }
      if (!statementPattern.getObjectVar.hasValue) {
        resultSet.addBinding(
          statementPattern.getObjectVar.getName,
          stmt.getObject
        )
      }
      if (
        statementPattern.getContextVar != null && !statementPattern.getContextVar.hasValue
      ) {
        resultSet.addBinding(
          statementPattern.getContextVar.getName,
          stmt.getContext
        )
      }
      resultSet
    }
  }

  implicit class BindingsExt(val bindings: Bindings) extends AnyVal {
    def getBindingsOf(resultSet: ResultSet): List[Option[Value]] = {
      bindings.map(k => Option(resultSet.getValue(k)))
    }

    def getBindingsForKeying(resultSet: ResultSet): List[Option[String]] = {
      getBindingsOf(resultSet).map(_.map(_.toString))
    }
  }

  implicit class BindingSetExt(val bindingSet: BindingSet) extends AnyVal {
    def toResultSet: ResultSet = bindingSet
  }

  implicit class TupleExprExt(val expr: TupleExpr) extends AnyVal {
    def getJoinBindings: Set[String] =
      expr match {
        case ext: Extension =>
          val bindings = ext.getAssuredBindingNames.asScala.toSet
          ext.getElements.asScala.foldLeft(bindings) { (acc, x) =>
            acc + x.getName
          }
        case _ => expr.getAssuredBindingNames.asScala.toSet
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

  def prepareDataJoin(
      fullDataset: SCollection[Statement]
  )(leftExpr: TupleExpr, rightExpr: TupleExpr): (
      SCollection[(List[Option[String]], ResultSet)],
      SCollection[(List[Option[String]], ResultSet)]
  ) = {
    val leftBindings = leftExpr.getJoinBindings
    val rightBindings = rightExpr.getJoinBindings
    val commonBindings =
      (leftBindings & rightBindings).toList // Deterministic order when iterating
    val leftDataset = processOperation(fullDataset)(leftExpr)
    val rightDataset = processOperation(fullDataset)(rightExpr)
    val keyedLeft = leftDataset.keyBy(commonBindings.getBindingsForKeying)
    val keyedRight =
      rightDataset.keyBy(commonBindings.getBindingsForKeying)
    (keyedLeft, keyedRight)
  }

  def processOperation(fullDataset: SCollection[Statement])(
      tupleExpr: TupleExpr
  ): SCollection[ResultSet] = {
    val sc = fullDataset.context
    val results = tupleExpr match {
      case _: SingletonSet =>
        val bindingSet: BindingSet = new EmptyBindingSet
        sc.parallelize(Seq(bindingSet))
      case statementPattern: StatementPattern =>
        fullDataset
          .filter(statementPattern.matches)
          .map(statementPattern.convertToResultSet)
      case join: Join =>
        (join.getLeftArg, join.getRightArg) match {
          case (left: BindingSetAssignment, right) =>
            val rightDataset = processOperation(fullDataset)(right)
            rightDataset.filter { resultSet =>
              left.getBindingSets.asScala.exists { assignedBindingSet =>
                resultSet.canBeJoined(
                  assignedBindingSet.nonNullBindings.map(_.getName),
                  assignedBindingSet
                )
              }
            }
          case (left, right: BindingSetAssignment) =>
            val leftDataset = processOperation(fullDataset)(left)
            leftDataset.filter { resultSet =>
              right.getBindingSets.asScala.exists { assignedBindingSet =>
                resultSet.canBeJoined(
                  assignedBindingSet.nonNullBindings.map(_.getName),
                  assignedBindingSet
                )
              }
            }
          case _ =>
            val (keyedLeft, keyedRight) =
              prepareDataJoin(fullDataset)(join.getLeftArg, join.getRightArg)
            keyedLeft.join(keyedRight).values.map {
              case (left, right) =>
                left ++ right
            }
        }
      case leftJoin: LeftJoin =>
        val leftDataset = processOperation(fullDataset)(leftJoin.getLeftArg)
        val rightDataset = {
          val rightData = processOperation(fullDataset)(leftJoin.getRightArg)
          if (leftJoin.hasCondition) {
            rightData.filter { resultSet =>
              resultSet
                .evaluateValueExpr(leftJoin.getCondition)
                .exists(
                  _.asInstanceOf[Literal]
                    .booleanValue()
                )
            }
          } else {
            rightData
          }
        }
        val leftBindings = leftJoin.getLeftArg.getJoinBindings
        val rightBindings = leftJoin.getRightArg.getJoinBindings
        val commonBindings =
          (leftBindings & rightBindings).toList // Deterministic order when iterating
        val keyedLeft = leftDataset.keyBy(commonBindings.getBindingsForKeying)
        val keyedRight =
          rightDataset.keyBy(commonBindings.getBindingsForKeying)
        keyedLeft.leftOuterJoin(keyedRight).values.map {
          case (left, right) =>
            left ++ right.getOrElse(EMPTY_RESULT_SET)
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
            case sample: Sample =>
              def reductionFunction(
                  left: (ResultSet, Option[Value]),
                  right: (ResultSet, Option[Value])
              ) = {
                (left, right) match {
                  case ((resultSet, x @ Some(_)), _) =>
                    resultSet -> x
                  case (_, (resultSet, y @ Some(_))) =>
                    resultSet -> y
                  case (_, (resultSet, _)) =>
                    resultSet -> None
                }
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(sample.getArg)
                  )
                  .collect {
                    case (key, resultSet -> literal) =>
                      key -> (resultSet -> literal)
                  }
              val samplePerKey = if (sample.isDistinct) {
                evaluatedExpr
                  .map {
                    case (key, pair @ (_, lit)) => (key, lit.toString) -> pair
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((key, _), pair) => key -> pair
                  }
                  .reduceByKey(reductionFunction)
              } else {
                evaluatedExpr.reduceByKey(reductionFunction)
              }
              samplePerKey.mapValues {
                case (resultSet, Some(aggregatedLiteral)) =>
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(bindingName, aggregatedLiteral)
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
                case (resultSet, _) =>
                  resultSet -> (new EmptyBindingSet).asInstanceOf[BindingSet]
              }
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
                    case (key, resultSet -> value) =>
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
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(
                    bindingName,
                    vf.createLiteral(aggregatedCount.toString, XSD.INTEGER)
                      .asInstanceOf[Value]
                  )
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
              }
            case sum: Sum =>
              def reductionFunction(
                  left: (ResultSet, Option[Literal]),
                  right: (ResultSet, Option[Literal])
              ) = {
                (left, right) match {
                  case ((resultSet, Some(x)), (_, Some(y))) =>
                    try {
                      resultSet -> Some(x + y)
                    } catch {
                      case ex: Throwable =>
                        resultSet -> None
                    }
                  case ((resultSet, _), _) => resultSet -> None
                }
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(sum.getArg)
                  )
                  .map {
                    case (key, resultSet -> Some(literal: Literal)) =>
                      key -> (resultSet -> Option(literal))
                    case (key, resultSet -> _) =>
                      key -> (resultSet -> None)
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
                case (resultSet, Some(aggregatedLiteral)) =>
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(bindingName, aggregatedLiteral)
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
                case (resultSet, _) =>
                  resultSet -> (new EmptyBindingSet).asInstanceOf[BindingSet]

              }
            case min: Min =>
              def reductionFunction(
                  left: (ResultSet, Option[Value]),
                  right: (ResultSet, Option[Value])
              ) = {
                (left, right) match {
                  case ((resultSet, Some(x)), (_, Some(y))) =>
                    try {
                      resultSet -> Some(x.min(y))
                    } catch {
                      case ex: Throwable =>
                        resultSet -> None
                    }
                  case ((resultSet, _), _) => resultSet -> None
                }
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(min.getArg)
                  )
                  .collect {
                    case (key, resultSet -> literal) =>
                      key -> (resultSet -> literal)
                  }
              val minPerKey = if (min.isDistinct) {
                evaluatedExpr
                  .map {
                    case (key, pair @ (_, lit)) => (key, lit.toString) -> pair
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((key, _), pair) => key -> pair
                  }
                  .reduceByKey(reductionFunction)
              } else {
                evaluatedExpr.reduceByKey(reductionFunction)
              }
              minPerKey.mapValues {
                case (resultSet, Some(aggregatedLiteral)) =>
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(bindingName, aggregatedLiteral)
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
                case (resultSet, _) =>
                  resultSet -> (new EmptyBindingSet).asInstanceOf[BindingSet]
              }
            case max: Max =>
              def reductionFunction(
                  left: (ResultSet, Option[Value]),
                  right: (ResultSet, Option[Value])
              ) = {
                (left, right) match {
                  case ((resultSet, Some(x)), (_, Some(y))) =>
                    try {
                      resultSet -> Some(x.max(y))
                    } catch {
                      case ex: Throwable =>
                        resultSet -> None
                    }
                  case ((resultSet, _), _) => resultSet -> None
                }
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(max.getArg)
                  )
                  .collect {
                    case (key, resultSet -> literal) =>
                      key -> (resultSet -> literal)
                  }
              val maxPerKey = if (max.isDistinct) {
                evaluatedExpr
                  .map {
                    case (key, pair @ (_, lit)) => (key, lit.toString) -> pair
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((key, _), pair) => key -> pair
                  }
                  .reduceByKey(reductionFunction)
              } else {
                evaluatedExpr.reduceByKey(reductionFunction)
              }
              maxPerKey.mapValues {
                case (resultSet, Some(aggregatedLiteral)) =>
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(bindingName, aggregatedLiteral)
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
                case (resultSet, _) =>
                  resultSet -> (new EmptyBindingSet).asInstanceOf[BindingSet]
              }
            case groupConcat: GroupConcat =>
              val separator =
                EMPTY_RESULT_SET
                  .evaluateValueExpr(groupConcat.getSeparator)
                  .getOrElse(vf.createLiteral(" "))
              def reductionFunction(
                  left: (ResultSet, Option[Value]),
                  right: (ResultSet, Option[Value])
              ) = {
                (left, right) match {
                  case ((resultSet, Some(x)), (_, Some(y))) =>
                    try {
                      resultSet -> Some(
                        ValueEvaluators.concat(
                          x.castToString,
                          separator,
                          y.castToString
                        )
                      )
                    } catch {
                      case ex: Throwable =>
                        resultSet -> None
                    }
                  case ((resultSet, _), _) => resultSet -> None
                }
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(groupConcat.getArg)
                  )
                  .map {
                    case (key, resultSet -> literal) =>
                      key -> (resultSet -> literal)
                  }
              val concatPerKey = if (groupConcat.isDistinct) {
                evaluatedExpr
                  .map {
                    case (key, pair @ (_, lit)) => (key, lit.toString) -> pair
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((key, _), pair) => key -> pair
                  }
                  .reduceByKey(reductionFunction)
              } else {
                evaluatedExpr.reduceByKey(reductionFunction)
              }
              concatPerKey.mapValues {
                case (resultSet, Some(aggregatedLiteral)) =>
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(bindingName, aggregatedLiteral)
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
                case (resultSet, _) =>
                  resultSet -> (new EmptyBindingSet).asInstanceOf[BindingSet]
              }
            case avg: Avg =>
              def reductionFunction(
                  left: (ResultSet, Option[AverageHelper]),
                  right: (ResultSet, Option[AverageHelper])
              ) = {
                (left, right) match {
                  case ((resultSet, Some(x)), (_, Some(y))) =>
                    resultSet -> (x + y)
                  case ((resultSet, _), _) => resultSet -> None
                }
              }
              val evaluatedExpr =
                keyedResults
                  .mapValues(resultSet =>
                    resultSet -> resultSet.evaluateValueExpr(avg.getArg)
                  )
                  .collect {
                    case (key, resultSet -> Some(literal: Literal)) =>
                      key -> (resultSet -> Some(
                        AverageHelper(literal, LITERAL_ONE)
                      ))
                    case (key, resultSet -> _) =>
                      key -> (resultSet -> None)
                  }
              val avgPerKey = if (avg.isDistinct) {
                evaluatedExpr
                  .map {
                    case (key, pair @ (_, lit)) => (key, lit.toString) -> pair
                  }
                  .reduceByKey(reductionFunction)
                  .map {
                    case ((key, _), pair) => key -> pair
                  }
                  .reduceByKey(reductionFunction)
              } else {
                evaluatedExpr.reduceByKey(reductionFunction)
              }
              avgPerKey.mapValues {
                case (resultSet, Some(aggregatedLiteral)) =>
                  val aggregatedResultSet = new MapBindingSet(1)
                  aggregatedResultSet.addBinding(
                    bindingName,
                    aggregatedLiteral.currentValue
                  )
                  resultSet -> aggregatedResultSet.asInstanceOf[BindingSet]
                case (resultSet, _) =>
                  resultSet -> (new EmptyBindingSet).asInstanceOf[BindingSet]
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
                resultSet.filter((x, _) => bindingsToGroupBy.contains(x))
              groupingBindings ++ value
          }
      case bindingSetAssignment: BindingSetAssignment =>
        val bindings =
          bindingSetAssignment.getBindingSets.asScala.map(_.toResultSet)
        sc.parallelize(bindings)
      case extension: Extension =>
        val results = processOperation(fullDataset)(extension.getArg)
        val extraValues = extension.getArg match {
          // Special case: RDF4J wraps the group by with an Extension that does work already done in the Group case.
          // This removes the elements already defined in the Group operator so that they don't crash the interpreter
          case group: Group =>
            val elementsToRemove =
              group.getGroupElements.asScala.toList.map(_.getOperator)
            val extensionElements = extension.getElements.asScala.toList
            extensionElements.filterNot { elem =>
              elementsToRemove.exists(op => elem.getExpr.equals(op))
            }
          case _ => extension.getElements.asScala
        }
        results.map { resultSet =>
          val extensionValues =
            extraValues
              .map(exElem =>
                exElem.getName -> resultSet.evaluateValueExpr(exElem.getExpr)
              )
              .foldLeft(new MapBindingSet()) {
                case (acc, (name, Some(value))) =>
                  acc.addBinding(name, value)
                  acc
                case (acc, _) => acc
              }
          resultSet ++ extensionValues
        }
      case filter: Filter =>
        filter.getCondition match {
          case exists: Exists =>
            val (keyedLeft, keyedRight) =
              prepareDataJoin(fullDataset)(filter.getArg, exists.getSubQuery)
            keyedLeft.join(keyedRight).values.collect {
              case (left, _) =>
                left
            }
          case not: Not if not.getArg.isInstanceOf[Exists] =>
            val exists = not.getArg.asInstanceOf[Exists]
            val (keyedLeft, keyedRight) =
              prepareDataJoin(fullDataset)(filter.getArg, exists.getSubQuery)
            keyedLeft.leftOuterJoin(keyedRight).values.collect {
              case (left, None) =>
                left
            }
          case _ =>
            val results = processOperation(fullDataset)(filter.getArg)
            results.filter { resultSet =>
              resultSet
                .evaluateValueExpr(filter.getCondition)
                .exists(_.asInstanceOf[Literal].booleanValue())
            }
        }

    }
    results
  }
}
