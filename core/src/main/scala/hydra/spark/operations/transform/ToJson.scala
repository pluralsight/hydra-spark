package hydra.spark.operations.transform

import hydra.spark.api._
import org.apache.spark.sql.catalyst.expressions.ColumnToJson
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by alexsilva on 1/25/17.
  */
case class ToJson(columns: Seq[String]) extends DFOperation {

  override def transform(df: DataFrame): DataFrame = {
    val jsonCols = columns.map(c => c -> new Column(ColumnToJson.expr(Map.empty, df.col(c)).get))

    jsonCols.foldLeft(df)((df, col) => df.withColumn(col._1, col._2))
  }

  override def validate: ValidationResult = {
    if (columns.isEmpty)
      Invalid(ValidationError("select-columns", "Column list cannot be empty."))
    else Valid
  }
}

