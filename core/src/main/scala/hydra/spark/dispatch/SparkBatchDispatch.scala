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
import org.apache.spark.sql.SparkSession

/**
  * Created by alexsilva on 6/20/16.
  */
case class SparkBatchDispatch[S](override val name: String, source: Source[S], operations: Operations,
                                 dsl: Config, sparkSession: SparkSession)
  extends SparkDispatch[S](name, source, operations, dsl, sparkSession) {


  override def run(): Unit = {
    val sqlContext = sparkSession.sqlContext
    val ops = operations.steps
    val initialDf = source.createDF(sqlContext)
    ops.foldLeft(initialDf)((df, trans) => trans.transform(df))
    source.checkpoint(None)
  }

  override def stop(): Unit = sparkSession.stop()
}