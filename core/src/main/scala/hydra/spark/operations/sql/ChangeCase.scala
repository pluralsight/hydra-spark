package hydra.spark.operations.sql

import com.google.common.base.CaseFormat
import com.typesafe.config.Config
import hydra.spark.api.{DFOperation, Valid, ValidationResult}
import org.apache.spark.sql.DataFrame

case class ChangeCase(from: CaseFormat, to: CaseFormat) extends DFOperation {

  override def transform(df: DataFrame): DataFrame = {
    df.columns.toSeq.foldLeft(df)((df, name) =>
      df.withColumnRenamed(name, from.to(to,name)))
  }

  override def validate: ValidationResult = Valid
}

object ChangeCase {
  import configs.syntax._

  def apply(cfg: Config): ChangeCase = {
    val from = cfg.get[CaseFormat]("from").valueOrThrow(_ => new IllegalArgumentException("A from value is required."))
    val to = cfg.get[CaseFormat]("to").valueOrThrow(_ => new IllegalArgumentException("A to value is required."))
    ChangeCase(from, to)
  }
}