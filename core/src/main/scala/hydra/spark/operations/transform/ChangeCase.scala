package hydra.spark.operations.transform

import com.google.common.base.CaseFormat
import com.typesafe.config.Config
import hydra.spark.api.{DFOperation, Invalid, Valid, ValidationResult}
import org.apache.spark.sql.DataFrame
import scala.util.Try

case class ChangeCase(from: String, to: String) extends DFOperation {

  override def transform(df: DataFrame): DataFrame = {
    val f = CaseFormat.valueOf(from)
    val t = CaseFormat.valueOf(to)
    df.columns.toSeq.foldLeft(df)((df, name) =>
      df.withColumnRenamed(name, f.to(t,name)))
  }

  override def validate: ValidationResult = {
    val r = checkRequiredParams(Seq("from"->from, "to"->to))
    r match {
      case Valid => validateCaseFormats(Seq(from, to))
      case r: Invalid => r
    }
  }

  def validateCaseFormats(values: Seq[String]): ValidationResult = {
    Try{
      values.map(CaseFormat.valueOf(_))
      Valid
    }.recover { case r => Invalid("change_case", r.getMessage)}.get
  }
}

object ChangeCase {
  import configs.syntax._

  def apply(cfg: Config): ChangeCase = {
    val from = cfg.get[String]("from").valueOrElse("")
    val to = cfg.get[String]("to").valueOrElse("")
    ChangeCase(from, to)
  }
}