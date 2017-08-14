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

/**
  * Created by alexsilva on 6/20/16.
  */
case class SparkBatchTransformation[S](override val name: String, source: Source[S], operations: Seq[DFOperation],
                                       dsl: Config)
  extends SparkTransformation[S](name, source, operations, dsl) {


  override def run(): Unit = {
    val ops = operations
    val initialDf = source.createDF(spark)
    ops.foldLeft(initialDf)((df, trans) => trans.transform(df))
    source.checkpoint(None)
  }

  override def stop(): Unit = spark.stop()
}