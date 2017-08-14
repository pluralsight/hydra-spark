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

package hydra.spark.dispatch

import com.typesafe.config.Config
import hydra.spark.api._
import configs.syntax._
import hydra.spark.configs.ConfigSupport
import hydra.spark.dsl.parser.TypesafeDSLParser
import org.apache.spark.sql.SparkSession

/**
  * Created by alexsilva on 6/21/16.
  */
abstract class SparkTransformation[S](name: String,
                                      source: Source[S],
                                      operations: Seq[DFOperation],
                                      dsl: Config) extends Transformation[S] with ConfigSupport {


  lazy val spark = SparkSession.builder().config(sparkConf(dsl, name)).getOrCreate()

  val author = dsl.get[String]("author").valueOrElse("Unknown")
}

object SparkTransformation {

  import scala.reflect.runtime.universe._

  val parser = TypesafeDSLParser(Seq("hydra.spark.sources"), Seq("hydra.spark.operations"))

  def apply(dsl: String): SparkTransformation[_] = {
    apply(parser.parse(dsl).get)
  }

  def apply[S: TypeTag](d: TransformationDetails[S]): SparkTransformation[S] = {

    if (d.isStreaming)
      SparkStreamingTransformation[S](d.name, d.source, d.operations, d.dsl)
    else
      SparkBatchTransformation[S](d.name, d.source, d.operations, d.dsl)
  }

}
