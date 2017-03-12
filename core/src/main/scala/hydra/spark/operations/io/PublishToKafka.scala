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

import com.typesafe.config.{Config, ConfigFactory}
import hydra.spark.api.{DFOperation, Invalid, Valid, ValidationResult}
import hydra.spark.avro.SchemaRegistrySupport
import hydra.spark.configs._
import hydra.spark.kafka.types.{AvroMessage, JsonMessage, StringMessage}
import hydra.spark.util.Collections._
import hydra.spark.util.Network
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * Created by alexsilva on 8/9/16.
  */
case class PublishToKafka(topic: String, format: String = "json", orderBy: Option[String] = None,
                          key: Option[String] = None, columns: Option[List[String]] = None,
                          properties: Map[String, String]) extends DFOperation with Logging with SchemaRegistrySupport {

  val cfg = ConfigFactory.defaultReference.withFallback(ConfigFactory.load(getClass.getClassLoader, "reference"))

  val topicDefaults = cfg.get[Map[String, String]](s"hydra.common.kafka.formats.$format").getOrElse(Map.empty)

  val kafkaDefaults = cfg.get[Map[String, String]]("kafka.producer").getOrElse(Map.empty)

  val opProps = topicDefaults ++ kafkaDefaults ++ properties

  override def id: String = s"kafka-$topic"

  lazy val schema = getValueSchema(topic)

  override def transform(df: DataFrame): DataFrame = {
    val broadcastedConfig = df.sqlContext.sparkContext.broadcast(opProps)

    val cdf = dropColumns(df)

    //TODO: allow other formats
    toOrderedDF(cdf).toJSON.foreach(json => {
      val producer: Producer[Any, Any] = {
        if (ProducerObject.isCached) ProducerObject.getCachedProducer
        else {
          val producer = new Producer[Any, Any](new ProducerConfig(broadcastedConfig.value))
          ProducerObject.cacheProducer(producer)
          producer
        }
      }

      val msg = kafkaMessage(format, getKey(json), dropKey(json))
      producer.send(new KeyedMessage[Any, Any](topic, msg.key, msg.payload))
    })

    df
  }

  private def dropKey(json: String): String = {
    val msg = key.map { k =>
      import spray.json._
      val fields = json.parseJson.asJsObject.fields
      JsObject(fields - k).compactPrint
    }.getOrElse(json)

    msg
  }

  private def dropColumns(df: DataFrame) = {
    columns.map(c => df.select(c.head, c.drop(1): _*)).getOrElse(df)
  }

  private def toOrderedDF(df: DataFrame) = {
    import org.apache.spark.sql.functions._
    orderBy.map(expr => {
      val parts = expr.trim().split(" ")
      val colName = parts(0)
      val dirStr = if (parts.size > 1) parts(1) else "asc"
      if (dirStr.equals("asc"))
        df.sort(asc(colName)).repartition(1)
      else
        df.sort(desc(colName)).repartition(1)
    }).getOrElse(df)
  }

  override def validate: ValidationResult = {
    Try {
      require(!topic.isEmpty, "A topic is required")
      require(properties.contains("metadata.broker.list"), "Metadata broker list is required.")
      properties("metadata.broker.list").toString.split(",").foreach { host =>
        val url = host.split(":")
        require(Network.isAlive(url(0), url(1).toInt), s"Connection to $host refused.")
      }
      Valid
    }.recover { case t: Throwable => Invalid("kafka", t.getMessage) }.get
  }

  def kafkaMessage(format: String, key: String, payload: String) = {
    if (format.equalsIgnoreCase("string")) StringMessage(key, payload)
    else if (format.equalsIgnoreCase("json")) JsonMessage(key, payload)
    else AvroMessage(schema, key, payload)
  }

  val getKey: (String) => String = (r) => key.map { k =>
    import spray.json._
    val jsonAst = r.parseJson
    jsonAst.asJsObject.fields.get(k) match {
      case Some(key) => key.compactPrint.replaceAll("^\"|\"$", "")
      case None => throw new IllegalArgumentException(s"Key $k not found.")
    }
  }.getOrElse("")
}

object ProducerObject {

  private var producerOpt: Option[Producer[Any, Any]] = None

  def getCachedProducer: Producer[Any, Any] = producerOpt.get

  def cacheProducer(producer: Producer[Any, Any]): Unit = producerOpt = Some(producer)

  def isCached: Boolean = producerOpt.isDefined
}

object PublishToKafka {

  def apply(cfg: Config): PublishToKafka = {
    val properties = cfg.get[Map[String, String]]("properties").getOrElse(Map.empty)
    val topic = cfg.get[String]("topic").getOrElse(throw new IllegalArgumentException("Topic is required."))
    val orderBy = cfg.get[String]("orderBy")
    val key = cfg.get[String]("key")
    val format = cfg.get[String]("format").getOrElse("json")
    val columns = cfg.get[List[String]]("columns")

    PublishToKafka(topic, format, orderBy, key, columns, properties)
  }
}