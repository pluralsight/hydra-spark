package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest._

/**
  * Created by alexsilva on 1/31/17.
  */
class StructToJsonSpec extends Matchers with FunSpecLike with RowHelper {

  describe("When converting a Array[StructType] to JSON") {
    it("converts to json") {
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val struct = Literal.create(createRow(1), schema)
      check(StructToJson(Map.empty, struct), """{"a":1}""")
    }
    it("is valid for struct types") {
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val struct = Literal.create(createRow(1), schema)
      val checkDT = StructToJson(Map.empty, struct).checkInputDataTypes()
      checkDT shouldBe a[TypeCheckSuccess.type]
    }

    it("to_json null input column") {
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val struct = Literal.create(null, schema)
      check(StructToJson(Map.empty, struct), null)
    }

    it("is invalid for non-struct types") {
      val str = Literal.create("test string", StringType)
      val checkDT = StructToJson(Map.empty, str).checkInputDataTypes()
      checkDT shouldBe a[TypeCheckFailure]
    }
  }

  def check(expression: => Expression, expected: Any, inputRow: InternalRow = EmptyRow) = {
    val serializer = new JavaSerializer(new SparkConf()).newInstance
    val expr: Expression = serializer.deserialize(serializer.serialize(expression))
    val catalystValue = CatalystTypeConverters.convertToCatalyst(expected)
    expr.eval(inputRow) shouldBe catalystValue
  }
}
