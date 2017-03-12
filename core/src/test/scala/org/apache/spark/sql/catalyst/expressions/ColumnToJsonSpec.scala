package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Column}
import org.scalatest._

/**
  * Created by alexsilva on 1/31/17.
  */
class ColumnToJsonSpec extends Matchers with FunSpecLike with RowHelper {

  describe("When using ColumnToJson") {
    it("handles a struct type") {
      val schema = StructType(StructField("a", IntegerType) :: Nil)
      val struct = Literal.create(createRow(1), schema)
      val expr = ColumnToJson.expr(Map.empty, Column(struct))
      expr.get shouldBe a[StructToJson]
    }
    it("handles an array or struct types") {
      val schema = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
      val struct = Literal.create(createRow(1), schema)
      val expr = ColumnToJson.expr(Map.empty, Column(struct))
      expr.get shouldBe a[StructArrayToJson]
    }
    it("is invalid for non-struct types") {
      val schema = StringType
      val struct = Literal.create(createRow(1), schema)
      val expr = ColumnToJson.expr(Map.empty, Column(struct))
      intercept[AnalysisException] {
        expr.get
      }
    }
    it("is invalid for an array of non-struct types") {
      val schema = ArrayType(StringType)
      val struct = Literal.create(createRow(1), schema)
      val expr = ColumnToJson.expr(Map.empty, Column(struct))
      intercept[AnalysisException] {
        expr.get
      }
    }
  }
}
