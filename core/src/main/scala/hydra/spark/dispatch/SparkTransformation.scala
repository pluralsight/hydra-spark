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
import configs.syntax._
import hydra.spark.api._
import hydra.spark.configs.ConfigSupport
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.internal.Logging
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.scheduler._
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import scala.util.Random

/**
  * Created by alexsilva on 6/21/16.
  */
abstract class SparkTransformation[S](source: Source[S],
                                      operations: Seq[DFOperation],
                                      dsl: Config) extends SparkListener with Transformation[S]
  with ConfigSupport with Logging {

  override lazy val name = dsl.get[String]("name").valueOrElse(Random.nextString(10))

  lazy val spark = {
    val sp = SparkSession.builder().config(sparkConf(dsl, name)).getOrCreate()
    sp.sparkContext.addSparkListener(this)
    sp
  }

  lazy val hydraContext = new HydraContext(spark.sparkContext)

  val author = dsl.get[String]("author").valueOrElse("Unknown")

  def init(): Unit = {
    (operations :+ source) foreach { op =>
      if (classOf[SparkListener].isAssignableFrom(op.getClass)) {
        spark.sparkContext.addSparkListener(op.asInstanceOf[SparkListener])
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Application ended.")
    (operations) foreach { op =>
      println(op)
    }
  }

  /** Listener methods **/
  override def onStageSubmitted(s: SparkListenerStageSubmitted): Unit = {
    operations.find(op => shouldFireEvent(s.stageInfo, op)).foreach(_.preTransform(hydraContext))
  }

  override def onStageCompleted(s: SparkListenerStageCompleted): Unit = {
    operations.find(op => shouldFireEvent(s.stageInfo, op)).foreach { op =>
      log.info(s"Firing stage completed for $id")
      val recordsWritten = s.stageInfo.taskMetrics.outputMetrics.recordsWritten
      val props = op.onStageCompleted(hydraContext)
      val counters = hydraContext.metrics.getCounters(op.getClass).mapValues(_.value.toLong)
      val opm = OperationMetrics(op.id, DateTime.now().toString, recordsWritten, props, counters)
      hydraContext.metrics.addOperationMetric(op, opm)
    }
  }

  private[spark] def shouldFireEvent(stageInfo: StageInfo, operation: DFOperation) = {
    //todo: is there a better way to get the operation name and match it to a stage info?
    stageInfo.name.contains(ClassUtils.getShortCanonicalName(operation.getClass))
  }
}

object SparkTransformation {

  import scala.reflect.runtime.universe._

  val parser = TypesafeDSLParser(Seq("hydra.spark.sources"), Seq("hydra.spark.operations"))

  def apply(dsl: String): SparkTransformation[_] = {
    apply(parser.parse(dsl).get)
  }

  def apply[S: TypeTag](d: TransformationDetails[S]): SparkTransformation[S] = {

    if (d.isStreaming)
      SparkStreamingTransformation[S](d.source, d.operations, d.dsl)
    else
      SparkBatchTransformation[S](d.source, d.operations, d.dsl)
  }

}
