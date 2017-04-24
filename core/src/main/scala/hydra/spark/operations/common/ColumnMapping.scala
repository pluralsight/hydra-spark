package hydra.spark.operations.common

import org.apache.spark.sql.types.DataType

/**
  * Created by alexsilva on 4/24/17.
  */
case class ColumnMapping(source: String, target: String, `type`: DataType)