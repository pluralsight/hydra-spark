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

package hydra.spark.operations

import hydra.spark.api.{ DFOperation, ValidationResult }
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL

/**
 * Created by alexsilva on 8/22/16.
 */
case class ElasticSearchWriter(index: String, properties: Map[String, String] = Map.empty) extends DFOperation {

  override def id: String = s"eswriter-$index"

  override def transform(df: DataFrame): DataFrame = {
    JavaEsSparkSQL.saveToEs(df, index)
    df
  }

  override def validate: ValidationResult = checkRequiredParams(Seq("index" -> index))
}
