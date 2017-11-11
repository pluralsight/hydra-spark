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
import hydra.spark.internal.Logging
import hydra.spark.kafka.types.{AvroMessage, JsonMessage, StringMessage}
import hydra.spark.util.Network
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.DataFrame
import configs.syntax._
import scala.util.Try

/**
  * Created by alexsilva on 8/9/16.
  */
case class PublishToKafka(topic: String, format: String = "json", orderBy: Option[String] = None,
                          key: Option[String] = None, columns: Option[List[String]] = None,
                          properties: Map[String, String]) extends DFOperation with Logging with SchemaRegistrySupport {

  val cfg = ConfigFactory.defaultReference.withFallback(ConfigFactory.load(getClass.getClassLoader, "reference"))

  val topicDefaults = cfg.get[Config](s"hydra.kafka.formats.$format").valueOrElse(ConfigFactory.empty)
    .to[Map[String,String]]

  val kafkaDefaults = cfg.get[Config]("kafka.producer").valueOrElse(ConfigFactory.empty)
    .to[Map[String,String]]

  val opProps = topicDefaults ++ kafkaDefaults ++ properties

  override def id: String = s"kafka-$topic"

  lazy val schema = getValueSchema(topic)

  override def transform(df: DataFrame): DataFrame = {
    val broadcastedConfig = df.sqlContext.sparkContext.broadcast(opProps)

    val cdf = dropColumns(df)

    //TODO: allow other formats
    toOrderedDF(cdf).toJSON.foreach(json => {
      val producer: KafkaProducer[Any, Any] = {
        if (ProducerObject.isCached) ProducerObject.getCachedProducer
        else {
          import scala.collection.JavaConverters._
          val producer = new KafkaProducer[Any, Any](broadcastedConfig.value.map(k => k._1 -> k._2.asInstanceOf[AnyRef])
            .asJava)
          ProducerObject.cacheProducer(producer)
          producer
        }
      }

      val key = getKey(json)
      val msg = kafkaMessage(format, key, dropKey(json))
      producer.send(new ProducerRecord[Any, Any](topic, msg.key, msg.payload))
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
      val bsc = ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
      require(!topic.isEmpty, "A topic is required")
      println(properties)
      require(properties.contains(bsc), s"$bsc is required.")
      properties(bsc).toString.split(",").foreach { host =>
        val url = host.split(":")
        require(Network.isAlive(url(0), url(1).toInt), s"Connection to $host refused.")
      }
      Valid
    }.recover { case t: Throwable => Invalid("publish-to-kafka", t.getMessage) }.get
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

  private var producerOpt: Option[KafkaProducer[Any, Any]] = None

  def getCachedProducer: KafkaProducer[Any, Any] = producerOpt.get

  def cacheProducer(producer: KafkaProducer[Any, Any]): Unit = producerOpt = Some(producer)

  def isCached: Boolean = producerOpt.isDefined
}

object PublishToKafka {

  def apply(cfg: Config): PublishToKafka = {
    import hydra.spark.configs._
    val properties:Map[String,String] = cfg.get[Config]("properties").valueOrElse(ConfigFactory.empty())
      .to[Map[String, String]]
    val topic = cfg.get[String]("topic").valueOrElse(throw new IllegalArgumentException("Topic is required."))
    val orderBy = cfg.get[String]("orderBy").toOption
    val key = cfg.get[String]("key").toOption
    val format = cfg.get[String]("format").valueOrElse("json")
    val columns = cfg.get[List[String]]("columns").toOption

    PublishToKafka(topic, format, orderBy, key, columns, properties)
  }
}