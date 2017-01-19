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

package hydra.spark.api

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{ Durations, StreamingContext }

import scala.concurrent.duration.Duration
import scala.language.existentials

/**
 * Created by alexsilva on 6/17/16.
 */
trait Source[S] extends Validatable {

  def name: String = {
    val name = getClass.getSimpleName
    name.substring(0, name.lastIndexOf("Source")).toLowerCase
  }

  def createStream(sc: StreamingContext): DStream[S]

  def createDF(ctx: SQLContext): DataFrame

  /**
   * Called by dispatchers to signal progress of the underlying processing.
   *
   * This is called:
   * 1. in streaming job, every time a batch is run for every RDD in a DStream.
   * 2. In a batch job, at the end of the job, if it completed successfully.
   */
  def checkpoint(rdd: Option[RDD[S]]): Unit = {}

  /**
   * Converts an RDD of type S to a dataframe of the same type.
   * Implementations should use the type class RDDConversions
   *
   * @param rdd
   */
  def toDF(rdd: RDD[S]): DataFrame

  def toSparkDuration(d: Any) = {
    d match {
      case dur: Duration => Durations.seconds(dur.toSeconds)
      case x => Durations.seconds(Duration(x.toString).toSeconds)
    }
  }
}
