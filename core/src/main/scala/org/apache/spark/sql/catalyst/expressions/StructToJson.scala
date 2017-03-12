package org.apache.spark.sql.catalyst.expressions

import java.io.CharArrayWriter

import hydra.spark.json.{JacksonGenerator, JacksonUtils}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Converts a [[StructType]] to a json output string.
  *
  *
  * From org.apache.spark.sql.catalyst.expressions.jsonExpressions (Spark 2.1.0)
  *
  * Created by alexsilva on 1/25/17.
  */
case class StructToJson(options: Map[String, String], child: Expression)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  override def nullable: Boolean = true

  @transient
  lazy val writer = new CharArrayWriter()

  @transient
  lazy val gen = new JacksonGenerator(child.dataType.asInstanceOf[StructType], writer)

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[StructType]) {
      try {
        JacksonUtils.verifySchema(child.dataType.asInstanceOf[StructType])
        TypeCheckResult.TypeCheckSuccess
      } catch {
        case e: UnsupportedOperationException =>
          TypeCheckResult.TypeCheckFailure(e.getMessage)
      }
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"$prettyName requires that the expression is a struct expression.")
    }
  }

  override def nullSafeEval(row: Any): Any = {
    gen.write(row.asInstanceOf[InternalRow])
    gen.flush()
    val json = writer.toString
    writer.reset()
    UTF8String.fromString(json)
  }

  override def inputTypes: Seq[AbstractDataType] = StructType :: ArrayType :: Nil
}

