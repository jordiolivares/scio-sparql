package es.jolivar.scio.sparql

import org.eclipse.rdf4j.model.Literal
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp
import org.eclipse.rdf4j.query.algebra.evaluation.util.MathUtil

object ValueEvaluators {
  implicit class LiteralExt(val lit: Literal) extends AnyVal {
    def +(other: Literal): Literal = MathUtil.compute(lit, other, MathOp.PLUS)
    def -(other: Literal): Literal = MathUtil.compute(lit, other, MathOp.MINUS)
    def *(other: Literal): Literal =
      MathUtil.compute(lit, other, MathOp.MULTIPLY)
    def /(other: Literal): Literal = MathUtil.compute(lit, other, MathOp.DIVIDE)
  }
}
