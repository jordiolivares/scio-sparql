package es.jolivar.scio.sparql

import org.eclipse.rdf4j.model.{Literal, Value}
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp
import org.eclipse.rdf4j.query.algebra.evaluation.util.{
  MathUtil,
  ValueComparator
}

object ValueEvaluators {
  private val valueComparator = new ValueComparator()

  implicit class LiteralExt(val lit: Literal) extends AnyVal {
    def +(other: Literal): Literal = MathUtil.compute(lit, other, MathOp.PLUS)
    def -(other: Literal): Literal = MathUtil.compute(lit, other, MathOp.MINUS)
    def *(other: Literal): Literal =
      MathUtil.compute(lit, other, MathOp.MULTIPLY)
    def /(other: Literal): Literal = MathUtil.compute(lit, other, MathOp.DIVIDE)
  }

  implicit class ValueExt(val value: Value) extends AnyVal {
    def <(other: Value): Boolean = valueComparator.compare(value, other) < 0
    def ==(other: Value): Boolean = valueComparator.compare(value, other) == 0
    def >(other: Value): Boolean = valueComparator.compare(value, other) > 0
    def <=(other: Value): Boolean = valueComparator.compare(value, other) <= 0
    def >=(other: Value): Boolean = valueComparator.compare(value, other) >= 0
    def min(other: Value): Value = {
      if (value < other) {
        value
      } else {
        other
      }
    }
    def max(other: Value): Value = {
      if (value > other) {
        value
      } else {
        other
      }
    }
  }
}
