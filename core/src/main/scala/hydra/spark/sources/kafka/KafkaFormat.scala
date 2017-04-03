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

package hydra.spark.sources.kafka

import com.databricks.spark.avro.SchemaConvertersWrapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import hydra.spark.kafka.serialization.JsonDecoder
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.codehaus.jackson.node.TextNode

import scala.reflect.ClassTag

abstract class KafkaFormat[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  extends Serializable {

  def format: String

  def createDF(ctx: SQLContext, topic: String,
               topicProps: Map[String, Any],
               properties: Map[String, String],
               key: Option[K]): DataFrame = {
    val rdd = createRDD(ctx.sparkContext, topic, topicProps, properties, key)

    val fxn = rdd.map(_.value.toString)

    schema(topicProps).map(s => ctx.read.schema(s).json(fxn)) getOrElse ctx.read.json(fxn)
  }

  def createRDD(
                 ctx: SparkContext,
                 topic: String,
                 topicProps: Map[String, Any],
                 properties: Map[String, String],
                 key: Option[K]
               ): RDD[KafkaMessageAndMetadata[K, V]] = {

    SparkKafkaUtils
      .createRDD[K, V, KD, VD, KafkaMessageAndMetadata[K, V]](ctx, topic, topicProps, properties, format)
      .map(m => key.map(k => addKey(m, k)).getOrElse(m))
  }

  def createDStream(
                     ctx: StreamingContext,
                     topic: String,
                     topicProps: Map[String, Any],
                     properties: Map[String, String],
                     key: Option[K]
                   ): DStream[KafkaMessageAndMetadata[K, V]] = {

    SparkKafkaUtils.createDStream[K, V, KD, VD, KafkaMessageAndMetadata[K, V]](ctx, topic, topicProps, properties)
      .transform(rdd => rdd.map(m => key.map(k => addKey(m, k)).getOrElse(m)))

  }

  def schema(props: Map[String, Any]): Option[StructType] = None

  def addKey(mmd: KafkaMessageAndMetadata[K, V], key: K): KafkaMessageAndMetadata[K, V]

}

object KafkaJsonFormat extends KafkaFormat[String, JsonNode, StringDecoder, JsonDecoder] {

  override def format = "json"

  def addKey(md: KafkaMessageAndMetadata[String, JsonNode], key: String) = {
    md.copy(value = md.value.asInstanceOf[ObjectNode].put(key, md.key))
  }
}

object KafkaAvroFormat extends KafkaFormat[String, Object, StringDecoder, KafkaAvroDecoder] {

  import scala.collection.JavaConverters._

  override def format = "avro"

  val keyField = (key: String) =>
    new Field(key, Schema.create(org.apache.avro.Schema.Type.STRING), "The message key.", TextNode.valueOf(""))

  def addKey(md: KafkaMessageAndMetadata[String, Object], key: String) = {
    val record = md.value.asInstanceOf[GenericRecord]
    val schema = record.getSchema
    val fields = schema.getFields.asScala
    val newSchema = generateKeyedSchema(key, fields, schema.getName, schema.getNamespace)

    val newRecord = new GenericData.Record(newSchema)
    fields.foreach(field => newRecord.put(field.name(), record.get(field.name)))
    newRecord.put(key, md.key)
    md.copy(value = newRecord)
  }

  def generateKeyedSchema(key: String, fields: Seq[Schema.Field], name: String, namespace: String) = {
    val newFields = keyField(key) +: fields
    val ns = Schema.createRecord(name, "New schema with the message key", namespace, false)
    ns.setFields(newFields.map(f => new Field(f.name(), f.schema(), f.doc(), f.defaultValue())).asJava)
    ns
  }

  override def schema(props: Map[String, Any]): Option[StructType] = {
    props.get("schema").map { schema =>
      val smap: Map[String, String] = props.map(k => k._1 -> k._2.toString)
      val schemaType = SchemaConvertersWrapper.convert(schema.toString, smap)
      schemaType.asInstanceOf[StructType]
    }
  }
}

object KafkaStringFormat extends KafkaFormat[String, String, StringDecoder, StringDecoder] {

  import spray.json._

  override def format = "string"

  def addKey(md: KafkaMessageAndMetadata[String, String], key: String) = {
    val pj = md.value.parseJson.asJsObject
    val keyed = JsObject(pj.fields + (key -> JsString(md.key))).toString
    md.copy(value = keyed)
  }
}

