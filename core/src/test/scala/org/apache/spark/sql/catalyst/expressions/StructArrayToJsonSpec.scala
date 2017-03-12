package org.apache.spark.sql.catalyst.expressions

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{TypeCheckFailure, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types._
import org.scalatest._

/**
  * Created by alexsilva on 1/31/17.
  */
class StructArrayToJsonSpec extends Matchers with FunSpecLike with RowHelper {

  val innerStruct = StructType(StructField("f1", IntegerType, true) :: Nil)
  val schema = new StructType(Array(StructField("a", innerStruct, true)))

  describe("When converting a StructType to JSON") {
    ignore("converts to json") {
      val st: StructType = new StructType().add(StructField("a", IntegerType))
      val attrs = schema.fields.map(f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val encoder = RowEncoder(schema).resolve(attrs, new ConcurrentHashMap[String, AnyRef]())
      val data = Literal.create(encoder.toRow(Row(1)).asInstanceOf[UnsafeRow].getArray(0), schema)
      check(StructArrayToJson(Map.empty, data), """[{"a":1},{"a":2}]""")
    }
    it("is valid for struct types") {
      val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
      val struct = Literal.create(createRow(1, 2), schema)
      val checkDT = StructArrayToJson(Map.empty, struct).checkInputDataTypes()
      checkDT shouldBe a[TypeCheckSuccess.type]
    }

    it("to_json null input column") {
      val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
      val struct = Literal.create(null, schema)
      check(StructArrayToJson(Map.empty, struct), null)
    }

    it("is invalid for non-array struct types") {
      val str = Literal.create("test string", StringType)
      val checkDT = StructArrayToJson(Map.empty, str).checkInputDataTypes()
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
