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

package hydra.spark.sources

import com.typesafe.config.Config
import hydra.spark.api.{ Invalid, Source, Valid, ValidationResult }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SQLContext }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by alexsilva on 10/24/16.
 */
abstract class AbstractFileSource[K: ClassTag, V: ClassTag, F <: InputFormat[K, V]: ClassTag] extends Source[V] {

  protected def properties: Map[String, Any]

  protected def path: String

  val windowDuration = properties.get("streaming.window.duration").map(toSparkDuration)
  val slideDuration = properties.get("streaming.slide.duration").map(toSparkDuration)
  val defaultFilter = (p: Path) => !p.getName().startsWith(".")

  override def createStream(sc: StreamingContext): DStream[V] = {
    setHadoopConfig(sc.sparkContext)
    sc.fileStream[K, V, F](path).map(_._2)
  }

  override def createDF(ctx: SQLContext): DataFrame = {
    setHadoopConfig(ctx.sparkContext)
    fileToDF(ctx.read)
  }

  val fileToDF = (reader: DataFrameReader) => reader.text(path)

  protected def setHadoopConfig(ctx: SparkContext): Unit = {
    val hadoopConfig = ctx.hadoopConfiguration
    hadoopConfig.set("fs.s3n.awsAccessKeyId", properties.getOrElse("fs.s3n.awsAccessKeyId", "").toString)
    hadoopConfig.set("fs.s3n.awsSecretAccessKey", properties.getOrElse("fs.s3n.awsSecretAccessKey", "").toString)
  }

  override def validate: ValidationResult = {
    Try(require(path.length > 0, "A path is required.")).map(x => Valid)
      .recover { case t: Throwable => Invalid(name, t.getMessage) }.get
  }
}

object AbstractFileSource {

  import hydra.spark.configs._

  def fromConfig(cfg: Config): (String, Map[String, String]) = {
    val path = cfg.get[String]("path").getOrElse("")
    val properties = cfg.get[Map[String, String]]("properties").getOrElse(Map.empty[String, String])
    (path, properties)
  }

}