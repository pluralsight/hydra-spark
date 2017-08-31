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

import com.typesafe.config.{Config, ConfigFactory}
import hydra.spark.api._
import hydra.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

/**
  * Created by alexsilva on 6/20/16.
  */
case class SparkStreamingTransformation[S: TypeTag](source: Source[S],
                                                    operations: Seq[DFOperation],
                                                    dsl: Config)
  extends SparkTransformation[S](source, operations, dsl) with Logging {

  import configs.syntax._
  import hydra.spark.configs._

  import scala.collection.JavaConverters._

  val streamingConf = ConfigFactory.parseMap(dsl.flattenAtKey("streaming").asJava)
  val stopGracefully = streamingConf.get[Boolean]("streaming.stopGracefully").valueOrElse(true)
  val stopSparkContext = streamingConf.get[Boolean]("streaming.stopSparkContext").valueOrElse(true)

  val interval = streamingConf.get[FiniteDuration]("streaming.interval").map(d => Seconds(d.toSeconds)).toOption

  lazy val ssc = StreamingContext.getActiveOrCreate { () =>
    new StreamingContext(spark.sparkContext, interval.get)
  }

  override def run(): Unit = {

    val stream = source.createStream(ssc)

    stream.foreachRDD { rdd: RDD[S] =>
      if (!rdd.isEmpty()) {
        //todo: handle exceptions
        val idf: DataFrame = source.toDF(rdd)
        operations.foldLeft(idf)((df, trans) => trans.transform(df))
        source.checkpoint(Some(rdd))
      }
    }
    ssc.start()
  }


  override def awaitTermination(): Unit = ssc.awaitTermination()

  override def stop(): Unit = ssc.stop(stopSparkContext, stopGracefully)

  override def validate: ValidationResult = {
    interval.map(_ => super.validate)
      .getOrElse(Invalid(this, new IllegalArgumentException("No streaming interval was defined for a streaming job.")))
  }

}
