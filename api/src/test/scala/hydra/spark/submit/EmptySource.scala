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

package hydra.spark.submit

import hydra.spark.api.{Source, Valid, ValidationResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


/**
 * Just to test factories.
 *
 * Created by alexsilva on 10/21/16.
 */
case class EmptySource(testName: String) extends Source[Row] {

  override def name: String = "test"

  override def createStream(sc: StreamingContext): DStream[Row] = ???

  override def createDF(session: SparkSession): DataFrame = ???

  override def validate: ValidationResult = Valid

  override def toDF(rdd: RDD[Row]): DataFrame = ???
}

