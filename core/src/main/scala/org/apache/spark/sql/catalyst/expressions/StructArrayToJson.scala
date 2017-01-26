package org.apache.spark.sql.catalyst.expressions

import java.io.CharArrayWriter

import hydra.spark.json.{JacksonGenerator, JacksonUtils}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Try

/** Converts an [[ArrayType[StructType]] to a json output string.
  *
  * All the elements in the array need to have the same StructType definition.
  *
  * Created by alexsilva on 1/25/17.
  */
case class StructArrayToJson(options: Map[String, String], child: Expression)
  extends UnaryExpression with CodegenFallback with ExpectsInputTypes {

  override def nullable: Boolean = true

  @transient
  lazy val writer = new CharArrayWriter()

  lazy val st = child.dataType.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]

  @transient
  lazy val gen = new JacksonGenerator(st, writer)

  override def dataType: DataType = StringType

  override def checkInputDataTypes(): TypeCheckResult = {

    Try {
      val array = child.dataType.asInstanceOf[ArrayType]
      //todo: How to really validate this? The StructTypes can have different elements inside them.
      JacksonUtils.verifySchema(array.elementType.asInstanceOf[StructType])
      TypeCheckResult.TypeCheckSuccess
    }.recover {
      case e: Exception => TypeCheckResult.TypeCheckFailure(
        s"$prettyName requires that the expression is an Array<Struct> expression.")
    }.get
  }

  override def nullSafeEval(row: Any): Any = {
    val unsafe = row.asInstanceOf[UnsafeArrayData]
    val elems: Seq[UnsafeRow] = for (i <- 0 until unsafe.numElements()) yield unsafe.getStruct(i, st.fields.length)
    gen.writeArray {
      elems.foreach { x =>
        gen.write(x)
      }
    }
    gen.flush()
    val json = writer.toString
    writer.reset()
    val s = UTF8String.fromString(json)
    s
  }

  override def inputTypes: Seq[AbstractDataType] = StructType :: ArrayType :: Nil
}

object StructArrayToJson {
  def fromColumn(options: Map[String, String], col: Column): StructToJson = StructToJson(options, col.expr)
}

