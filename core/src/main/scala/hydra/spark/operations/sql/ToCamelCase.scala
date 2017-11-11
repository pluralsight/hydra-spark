package hydra.spark.operations.sql

import com.google.common.base.CaseFormat
import hydra.spark.api.{DFOperation, Valid, ValidationResult}
import org.apache.spark.sql.DataFrame

case class ToCamelCase() extends DFOperation {
  override def transform(df: DataFrame): DataFrame = {
    df.columns.toSeq.foldLeft(df)((df, name) =>
      df.withColumnRenamed(name, CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name)))
  }

  override def validate: ValidationResult = Valid
}