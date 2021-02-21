package es.jolivar.scio.sparql

import es.jolivar.scio.sparql.ValueEvaluators._
import org.eclipse.rdf4j.model.Literal

case class AverageHelper(currentValue: Literal, numElements: Literal) {
  private def expandedAverage: Literal = currentValue * numElements

  def +(other: AverageHelper): Option[AverageHelper] = {
    try {
      val newNumElements = numElements + other.numElements
      val newAverage =
        (expandedAverage + other.expandedAverage) / newNumElements
      val newHelper = AverageHelper(newAverage, newNumElements)
      Some(newHelper)
    } catch {
      case ex: Throwable =>
        None
    }
  }
}
