package es.jolivar.scio.sparql

import com.spotify.scio.values.SCollection
import es.jolivar.scio.sparql.ValueEvaluators._
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StateId}
import org.apache.beam.sdk.values.KV
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
  private val EMPTY_RESULT_SET: BindingSet = EmptyBindingSet.getInstance()
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

  private implicit class ResultSetExt(val resultSet: BindingSet)
      extends AnyVal {
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

    def ++(other: BindingSet): BindingSet = {
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

  private implicit class VarExt(val v: Var) extends AnyVal {
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

  private implicit class BindingsExt(val bindings: Bindings) extends AnyVal {
    def getBindingsOf(resultSet: BindingSet): List[Option[Value]] = {
      bindings.map(k => Option(resultSet.getValue(k)))
    }

    def getBindingsForKeying(resultSet: BindingSet): List[Option[String]] = {
      getBindingsOf(resultSet).map(_.map(_.toString))
    }
  }

  private implicit class TupleExprExt(val expr: TupleExpr) extends AnyVal {
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

  implicit class SCollectionStatements(val col: SCollection[Statement])
      extends AnyVal {
    def executeSparql(
        query: String
    ): SCollection[BindingSet] = {
      val parsedQuery =
        QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL, query, null)
      col.transform("Performing SPARQL Query") { c =>
        processOperation(c)(parsedQuery.getTupleExpr)
      }
    }

    private[Interpreter] def getMatchingStatements(
        subject: Option[Var],
        predicate: Option[Var],
        `object`: Option[Var],
        graph: Option[Var]
    ): SCollection[BindingSet] = {
      col
        .filter { stmt =>
          val graphMatches = graph.forall(_.matches(stmt.getContext))
          val subjectMatches = subject.forall(_.matches(stmt.getSubject))
          val predicateMatches =
            predicate.forall(_.matches(stmt.getPredicate))
          val objectMatches = `object`.forall(_.matches(stmt.getObject))
          graphMatches && subjectMatches && predicateMatches && objectMatches
        }
        .map { stmt =>
          val resultSet = new MapBindingSet()
          subject.foreach {
            case subject if !subject.hasValue =>
              resultSet.addBinding(
                subject.getName,
                stmt.getSubject
              )
            case _ =>
          }
          predicate.foreach {
            case predicate if !predicate.hasValue =>
              resultSet.addBinding(
                predicate.getName,
                stmt.getPredicate
              )
            case _ =>
          }
          `object`.foreach {
            case obj if !obj.hasValue =>
              resultSet.addBinding(
                obj.getName,
                stmt.getObject
              )
            case _ =>
          }
          graph.foreach {
            case graph if !graph.hasValue =>
              resultSet.addBinding(
                graph.getName,
                stmt.getContext
              )
            case _ =>
          }
          resultSet
        }
    }
  }

  private def prepareDataJoin(
      fullDataset: SCollection[Statement]
  )(leftExpr: TupleExpr, rightExpr: TupleExpr): (
      SCollection[(List[Option[String]], BindingSet)],
      SCollection[(List[Option[String]], BindingSet)]
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

  class OffsetStatefulDoFn[T](offset: Long)
      extends DoFn[KV[T, BindingSet], KV[T, BindingSet]] {
    @StateId("readCount") private val read =
      StateSpecs.value[java.lang.Long]() // scalafix:ok

    @ProcessElement
    def processElement(
        ctx: ProcessContext,
        @StateId("readCount") readCount: ValueState[java.lang.Long]
    ): Unit = {
      val c = readCount.read()
      val elem = ctx.element()
      if (c < offset) {
        readCount.write(c + 1)
      } else {
        ctx.output(elem)
      }
    }
  }

  def processOperation(fullDataset: SCollection[Statement])(
      tupleExpr: TupleExpr
  ): SCollection[BindingSet] = {
    val sc = fullDataset.context
    val results = tupleExpr match {
      case order: Order =>
        processOperation(fullDataset)(order.getArg)
      case slice: Slice
          if !slice.getArg
            .asInstanceOf[Projection]
            .getArg
            .isInstanceOf[Order] =>
        val results = processOperation(fullDataset)(slice.getArg)
        val offsetResults = results
          .map(0 -> _)
          .applyPerKeyDoFn(new OffsetStatefulDoFn(slice.getOffset))
          .map(_._2)
        if (slice.hasLimit) {
          offsetResults.take(slice.getLimit)
        } else {
          offsetResults
        }
      case slice: Slice
          if slice.getArg
            .asInstanceOf[Projection]
            .getArg
            .isInstanceOf[Order] =>
        val orderBy =
          slice.getArg.asInstanceOf[Projection].getArg.asInstanceOf[Order]
        val preOrdering = processOperation(fullDataset)(orderBy.getArg)
        val elements = orderBy.getElements.asScala.toList
        val ordering = new Ordering[(List[Option[Value]], BindingSet)] {
          def compare(
              x: (List[Option[Value]], BindingSet),
              y: (List[Option[Value]], BindingSet)
          ): Int = {
            val keyX = x._1
            val keyY = y._1
            elements.lazyZip(keyX).lazyZip(keyY).foldLeft(0) {
              case (-1, _) => -1
              case (1, _)  => 1
              // The ordering is switched according to the ASC/DESC modifier
              case (0, (orderElem, xElem, yElem)) =>
                if (orderElem.isAscending)
                  -xElem.orNull.compare(yElem.orNull)
                else
                  xElem.orNull.compare(yElem.orNull)
            }
          }
        }
        val numToSkip = math.max(slice.getOffset, 0)
        val numToReturn = math.max(slice.getLimit, 0)
        val numTopElements = numToReturn + numToSkip
        val topElements = preOrdering
          .map { bindingSet =>
            elements
              .map(x => bindingSet.evaluateValueExpr(x.getExpr)) -> bindingSet
          }
          .top(numTopElements.toInt)(ordering)

        if (numToReturn > 0) {
          topElements.map(x => x.drop(numToSkip.toInt)).flatten.values
        } else {
          val toIgnore = topElements.flatten.values.asSetSingletonSideInput
          preOrdering
            .withSideInputs(toIgnore)
            .filter {
              case (bindingSet, ctx) =>
                val resultsToSkip = ctx(toIgnore)
                !resultsToSkip.contains(bindingSet)
            }
            .toSCollection
        }
      case _: SingletonSet =>
        val bindingSet: BindingSet = new EmptyBindingSet
        sc.parallelize(Seq(bindingSet))
      case statementPattern: StatementPattern =>
        fullDataset.getMatchingStatements(
          Option(statementPattern.getSubjectVar),
          Option(statementPattern.getPredicateVar),
          Option(statementPattern.getObjectVar),
          Option(statementPattern.getContextVar)
        )
      case zeroLengthPath: ZeroLengthPath =>
        val (subjectVar, objectVar, graphVar) = (
          zeroLengthPath.getSubjectVar,
          zeroLengthPath.getObjectVar,
          Option(zeroLengthPath.getContextVar)
        )
        val filteredDataset = graphVar
          .filter(_.hasValue)
          .map(graph => fullDataset.filter(_.getContext == graph.getValue))
          .getOrElse(fullDataset)
        (subjectVar, objectVar, graphVar) match {
          case (subjectVar, objectVar, graph)
              if !subjectVar.hasValue && objectVar.hasValue =>
            val result = new MapBindingSet(2)
            result.addBinding(subjectVar.getName, objectVar.getValue)
            graph.foreach {
              case graphVar if graphVar.hasValue =>
                result.addBinding(graphVar.getName, graphVar.getValue)
              case _ =>
            }
            sc.parallelize(Seq[BindingSet](result))
          case (subjectVar, objectVar, graph)
              if subjectVar.hasValue && !objectVar.hasValue =>
            val result = new MapBindingSet(2)
            result.addBinding(objectVar.getName, subjectVar.getValue)
            graph.foreach {
              case graphVar if graphVar.hasValue =>
                result.addBinding(graphVar.getName, graphVar.getValue)
              case _ =>
            }
            sc.parallelize(Seq[BindingSet](result))
          case (subjectVar, objectVar, graph)
              if !subjectVar.hasValue && !objectVar.hasValue =>
            filteredDataset.flatMap { stmt =>
              val resultSubject = new MapBindingSet(3)
              resultSubject.addBinding(subjectVar.getName, stmt.getSubject)
              resultSubject.addBinding(objectVar.getName, stmt.getSubject)
              val resultObject = new MapBindingSet(3)
              resultObject.addBinding(subjectVar.getName, stmt.getObject)
              resultObject.addBinding(objectVar.getName, stmt.getObject)
              graph.foreach {
                case graphVar if graphVar.hasValue =>
                  resultSubject.addBinding(graphVar.getName, graphVar.getValue)
                  resultObject.addBinding(graphVar.getName, graphVar.getValue)
                case graphVar if stmt.getContext != null =>
                  resultSubject.addBinding(graphVar.getName, stmt.getContext)
                  resultObject.addBinding(graphVar.getName, stmt.getContext)
                case _ =>
              }
              Seq[BindingSet](resultSubject, resultObject)
            }
          case _ => sc.parallelize(Seq(EMPTY_RESULT_SET))
        }
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
                  left: (BindingSet, Option[Value]),
                  right: (BindingSet, Option[Value])
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
                  left: (BindingSet, Int),
                  right: (BindingSet, Int)
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
                  left: (BindingSet, Option[Literal]),
                  right: (BindingSet, Option[Literal])
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
                  left: (BindingSet, Option[Value]),
                  right: (BindingSet, Option[Value])
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
                  left: (BindingSet, Option[Value]),
                  right: (BindingSet, Option[Value])
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
                  left: (BindingSet, Option[Value]),
                  right: (BindingSet, Option[Value])
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
                  left: (BindingSet, Option[AverageHelper]),
                  right: (BindingSet, Option[AverageHelper])
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
          bindingSetAssignment.getBindingSets.asScala
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
