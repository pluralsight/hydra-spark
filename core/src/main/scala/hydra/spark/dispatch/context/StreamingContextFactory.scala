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

package hydra.spark.dispatch.context

import com.typesafe.config.{Config, ConfigFactory}
import hydra.spark.api.{ContextLike, Dispatch, DispatchDetails, SparkContextFactory}
import hydra.spark.configs._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

/**
  * Created by alexsilva on 1/7/17.
  */
class StreamingContextFactory extends SparkContextFactory {

  type C = StreamingContext with ContextLike

  def makeContext(sparkConf: SparkConf, config: Config): C = {
    makeContext(SparkContext.getOrCreate(sparkConf), config)
  }

  def makeContext(ctx: SparkContext, config: Config) = {
    val streamingConf = ConfigFactory.parseMap(config.flattenAtKey("streaming").asJava)
    val interval = streamingConf.get[FiniteDuration]("streaming.interval").map(d => Seconds(d.toSeconds))
    val stopGracefully = streamingConf.get[Boolean]("streaming.stopGracefully").getOrElse(true)
    val stopSparkContext = streamingConf.get[Boolean]("streaming.stopSparkContext").getOrElse(true)

    val ssc = new StreamingContext(ctx, interval.get) with ContextLike {
      def isValidDispatch(job: DispatchDetails[_]): Boolean = job.isStreaming

      def stop() = stop(stopSparkContext, stopGracefully)
    }

    for ((k, v) <- hadoopConfigs(config)) ssc.sparkContext.hadoopConfiguration.set(k, v)
    ssc
  }
}
