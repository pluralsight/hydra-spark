package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}
import org.apache.spark.sql.{AnalysisException, Column}

import scala.util.{Failure, Success, Try}

/**
  * Factory class that given a column decides if we should use
  * StructToJson or StructArrayToJson to do the conversion.
  *
  * Created by alexsilva on 1/26/17.
  */
object ColumnToJson {

  def expr(options: Map[String, String], column: Column): Try[Expression] = {
    val dt = column.expr.dataType
    val isStruct: (DataType) => Boolean = (dt) => dt.isInstanceOf[StructType]
    if (isStruct(dt))
      Success(StructToJson(options, column.expr))
    else if (dt.isInstanceOf[ArrayType] && isStruct(dt.asInstanceOf[ArrayType].elementType))
      Success(StructArrayToJson(options, column.expr))

    else Failure(new AnalysisException(s"Column must be a StructType or Array<StructType>.  It is a ${dt.typeName}"))
  }
}


