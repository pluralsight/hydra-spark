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

import com.typesafe.config.Config
import hydra.spark.api.{ContextLike, DispatchDetails, SparkContextFactory}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by alexsilva on 1/7/17.
  */
class DefaultSparkContextFactory extends SparkContextFactory {

  type C = SparkContext with ContextLike


  def makeContext(sparkConf: SparkConf, config: Config): C = {
    val sc = new SparkContext(sparkConf) with ContextLike {
      def sparkContext: SparkContext = this

      def isValidDispatch(job: DispatchDetails[_]): Boolean = !job.isStreaming
    }
    for ((k, v) <- hadoopConfigs(config)) sc.hadoopConfiguration.set(k, v)
    sc
  }
}

