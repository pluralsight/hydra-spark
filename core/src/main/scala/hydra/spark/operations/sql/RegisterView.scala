package hydra.spark.operations.sql

import hydra.spark.api.{DFOperation, ValidationResult}
import org.apache.spark.sql.DataFrame

/**
  * Created by alexsilva on 5/3/17.
  */
case class RegisterView(name: String, global: Boolean = false) extends DFOperation {
  override def transform(df: DataFrame): DataFrame = {
    if (global) df.createGlobalTempView(name) else df.createOrReplaceTempView(name)
    df
  }

  override def validate: ValidationResult = checkRequiredParams(Seq("view name" -> name))

}


