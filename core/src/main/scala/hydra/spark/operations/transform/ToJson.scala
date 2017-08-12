package hydra.spark.operations.transform

import hydra.spark.api._
import org.apache.spark.sql.DataFrame
/**
  * Created by alexsilva on 1/25/17.
  */
case class ToJson(columns: Seq[String]) extends DFOperation {

  override def transform(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.{to_json, col}
    import df.sqlContext.implicits._
    ifNotEmpty(df) { df =>
      columns.foldLeft(df)((df,c)=>df.withColumn(c, to_json(col(c)).as[String]))
    }
  }

  override def validate: ValidationResult = {
    if (columns.isEmpty)
      Invalid(ValidationError("select-columns", "Column list cannot be empty."))
    else Valid
  }
}