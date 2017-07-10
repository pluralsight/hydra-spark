package hydra.spark.util

import org.springframework.expression.common.TemplateParserContext
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.StandardEvaluationContext

/**
  * Created by alexsilva on 7/7/17.
  */
object Expressions {
  private val evalCtx = new StandardEvaluationContext()

  private val exprs = new com.pluralsight.hydra.expr.Expressions()

  def parseExpression(expr: String): String = {
    val parser = new SpelExpressionParser()
    parser.parseExpression(expr, new TemplateParserContext()).getValue(evalCtx, exprs, classOf[String])
  }

}

