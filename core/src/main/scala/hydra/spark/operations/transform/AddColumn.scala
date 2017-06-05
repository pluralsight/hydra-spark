/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.operations.transform

import hydra.spark.api.DFOperation
import hydra.spark.api.ValidationResult
import org.apache.spark.sql.DataFrame
import org.springframework.expression.spel.standard.SpelExpressionParser
/**
 * Created by alexsilva on 8/16/16.
 */
case class AddColumn(name: String, value: Any) extends DFOperation {
  override def id: String = s"add-column-$name"

  override def transform(df: DataFrame): DataFrame = {
    val isExpr = value.toString.startsWith("${")
    val resValue = if (isExpr) parseExpr(value.toString.substring(2, value.toString.length - 1)) else value
    df.withColumn(name, org.apache.spark.sql.functions.lit(resValue))
  }

  override def validate: ValidationResult = {
    checkRequiredParams(Seq(("name", name), ("value", value)))
  }

  private def parseExpr(expr: String): AnyRef = {
    val parser = new SpelExpressionParser()
    val exp = parser.parseExpression(expr)
    exp.getValue
  }
}
