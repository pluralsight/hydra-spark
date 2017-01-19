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

import com.typesafe.config.ConfigFactory
import hydra.spark.api._
import hydra.spark.dispatch.SparkStreamingDispatch
import org.apache.spark.streaming.StreamingContext

import scala.reflect.runtime.universe._

/**
 * Created by alexsilva on 1/3/17.
 */
case class StreamingTestDispatch[S: TypeTag](source: Source[S], operations: Operations, ssc: ContextLike)
    extends Dispatch[S] {

  val dsl = ConfigFactory.parseString(
    """
      |spark.master = "local[1]"
      |spark.default.parallelism	= 1
      |spark.ui.enabled = false
      |spark.driver.allowMultipleContexts = false
      |batchDuration = 1s
      |spark.checkpoint = false
    """.stripMargin
  )

  val dispatch = SparkStreamingDispatch("test", source, operations, dsl,ssc)

  def run() = dispatch.run()

  override def validate: ValidationResult = Valid

  override def stop(): Unit = dispatch.stop()

}