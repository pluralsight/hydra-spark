package hydra.spark.operations.transform

import hydra.spark.api.{DFOperation, ValidationResult}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
  * Created by dustinvannoy on 1/20/17.
  */
case class ConcatColumns(columnNames: Seq[String], newName: String) extends DFOperation {
  override def id: String = s"concat-columns-$newName"

  override def transform(df: DataFrame): DataFrame = {
    import df.sqlContext.implicits.StringToColumn
    //Define a udf to concatenate two passed in string values
    ifNotEmpty(df) { df =>
      val getConcatenated = udf((col1: String, col2: String) => {
        col1 + "|" + col2
      })
      df.withColumn(newName, getConcatenated(columnNames.map(c => $"$c"): _*))
    }
  }

  override def validate: ValidationResult = {
    checkRequiredParams(Seq(("columnNames", columnNames), ("newName", newName)))
  }

}
