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

package hydra.spark.testutils

import com.typesafe.config.Config
import hydra.spark.api.{Source, Valid}
import hydra.spark.util.RDDConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
 * Created by alexsilva on 11/21/16.
 */
case class ListSource(seq: Seq[String]) extends Source[String] {

  override def validate = Valid

  override def createStream(sc: StreamingContext): DStream[String] = {
    val rdd = sc.sparkContext.parallelize(seq, 1)
    val lines = mutable.Queue[RDD[String]](rdd)
    sc.queueStream[String](lines, false, rdd)
  }

  override def createDF(ctx: SparkSession): DataFrame =
    ctx.read.json(ctx.sparkContext.parallelize(seq))

  override def toDF(rdd: RDD[String]): DataFrame = rdd.toDF
}

object ListSource {

  import scala.collection.JavaConverters._

  def apply(cfg: Config): ListSource = ListSource(cfg.getStringList("messages").asScala)
}