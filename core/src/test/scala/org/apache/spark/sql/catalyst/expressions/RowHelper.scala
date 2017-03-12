package org.apache.spark.sql.catalyst.expressions


import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}

/**
  * Created by alexsilva on 1/31/17.
  */
trait RowHelper {
  def createRow(values: Any*): InternalRow =
    InternalRow.fromSeq(values.map(CatalystTypeConverters.convertToCatalyst))


}
