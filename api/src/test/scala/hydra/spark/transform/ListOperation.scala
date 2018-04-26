package hydra.spark.transform

import hydra.spark.api.{DFOperation, Valid, ValidationResult}
import org.apache.spark.sql.DataFrame

object ListOperation extends DFOperation {
  override def id: String = "ListOperation"

  val l = new scala.collection.mutable.ListBuffer[String]()

  def reset() = l.clear()

  override def transform(df: DataFrame): DataFrame = {
    df.toJSON.collect.foreach { s => l += s }
    df
  }

  override def validate: ValidationResult = Valid
}