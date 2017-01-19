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

package hydra.spark.operations.io

import java.io.File

import com.pluralsight.hydra.common.avro.JsonConverter
import com.typesafe.config.Config
import hydra.spark.api._
import hydra.spark.avro.SchemaRegistrySupport
import hydra.spark.internal.Logging
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroJob, AvroKeyOutputFormat}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by alexsilva on 6/21/16.
  */
case class SaveAsAvro(directory: String, schema: String, codec: Option[String], properties: Map[String, String])
  extends DFOperation with SchemaRegistrySupport with Logging {

  override val id: String = s"save-as-avro-$directory-$codec"

  private lazy val avroSchema: Schema = getValueSchema(schema)

  override def transform(df: DataFrame): DataFrame = {
    val path = new File(directory, System.currentTimeMillis.toString)
    saveHadoopAvro[String](df.toJSON, new Path(directory, System.currentTimeMillis().toString))
    df
  }

  private def saveHadoopAvro[A: ClassTag](rdd: RDD[A], path: Path): Unit = {

    val job: Job = Job.getInstance()
    AvroJob.setOutputKeySchema(job, avroSchema)

    codec.foreach { c =>
      job.getConfiguration.set("mapreduce.output.compress", "true")
      job.getConfiguration.set("mapred.output.compression.codec", c)
    }

    val schemaAsString = avroSchema.toString(false)

    rdd.mapPartitions(tuples => {
      val innerSchema = new Schema.Parser().parse(schemaAsString)
      tuples.map(tuple => {
        val reader = new GenericDatumReader[GenericRecord](innerSchema)
        val converter = new JsonConverter[GenericRecord](innerSchema)
        converter.convert(tuple.toString)
      })
    }).map(r => (new AvroKey[GenericRecord](r), NullWritable.get()))
      .saveAsNewAPIHadoopFile(
        path.toString,
        classOf[AvroKey[A]],
        classOf[NullWritable],
        classOf[AvroKeyOutputFormat[A]],
        job.getConfiguration
      )
  }

  override def validate: ValidationResult = {
    Try {
      checkRequiredParams(Seq(("schema", schema)))
      //force parsing
      log.debug(s"Using schema: ${avroSchema.getFullName}")
      Valid
    }.recover { case t => Invalid("avro", t.getMessage) }.get
  }
}

object SaveAsAvro {
  def apply(cfg: Config): SaveAsAvro = {
    import hydra.spark.configs._
    val props = cfg.get[Map[String, String]]("properties").getOrElse(Map.empty)
    SaveAsAvro(cfg.getString("directory"), cfg.getString("schema"), cfg.get[String]("codec"), props)
  }
}
