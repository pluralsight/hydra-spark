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

import com.typesafe.config.{Config, ConfigObject}
import hydra.spark.api._
import hydra.spark.util.RDDConversions._
import hydra.spark.util.{KafkaUtils, Network}
import kafka.api.OffsetRequest
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.HasOffsetRanges

import scala.util.Try

/**
  * Dispatch source that uses a Spark Direct Stream to consume messages from Kafka.
  *
  * @param topics     A map where the key is the topic name and the value are the properties to be used for that topic,
  *                   which are:
  *                   format -> the topic format
  *                   start -> When to start streaming; can use -1(OffsetRequest.EarliestTime) or -2 (OffsetRequest
  *                   .LatestTime)
  * @param properties Any Kafka config property.
  *
  *                   Key columns: Adding a keyColumn property to topics, will add a column with the name specified by this parameter
  *                   to the dataframe and its value will be the KafkaKey.
  */
case class KafkaSource(topics: Map[String, Map[String, Any]], properties: Map[String, String])
  extends Source[KafkaMessageAndMetadata[_, _]] {

  type KMMD = KafkaMessageAndMetadata[_, _]

  override val name = "kafka"

  val topicFormats = Map(
    "json" -> KafkaJsonFormat,
    "avro" -> KafkaAvroFormat,
    "string" -> KafkaStringFormat
  )

  override def createDF(ctx: SQLContext): DataFrame = {
    val dfs = topics.map {
      case (topic, props) =>
        topicFormats(formatName(props)).createDF(ctx, topic, props, properties, props.get("keyColumn").map(_.toString))
    }
    dfs.reduceLeft(_.unionAll(_))
  }

  override def createStream(ctx: StreamingContext): DStream[KafkaMessageAndMetadata[_, _]] = {
    val streams = topics.map {
      case (topic, props) =>
        topicFormats(formatName(props))
          .createDStream(ctx, topic, props, properties, props.get("keyColumn").map(_.toString))
          .asInstanceOf[DStream[KafkaMessageAndMetadata[_, _]]]
    }.toSeq
    ctx.union(streams)
  }

  override def validate(): ValidationResult = {
    Try {
      require(topics.size >= 1, "At least one topic is required")
      require(properties.get("metadata.broker.list").isDefined, "Metadata broker list is required.")
      properties("metadata.broker.list").split(",").foreach { host =>
        val url = host.split(":")
        require(Network.isAlive(url(0), url(1).toInt), s"Connection to $host refused.")
      }
      Valid
    }.recover { case t: Throwable => Invalid("kafka", t.getMessage) }.get
  }

  private def formatName(props: Map[String, Any]) = props.get("format").getOrElse("avro").toString

  override def checkpoint(rdd: Option[RDD[KMMD]]): Unit = {
    Try {
      val hasOffsetRanges = rdd.isDefined && rdd.get.isInstanceOf[HasOffsetRanges]
      if (hasOffsetRanges) {
        val offsetRanges = rdd.get.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.map { range =>
          val topic = range.topic
          val format = formatName(topics(topic))
          val cfg = new kafka.consumer.ConsumerConfig(SparkKafkaUtils.consumerConfig(format, properties))
          val offsets: Map[Int, (Long, Long)] = Map(range.partition -> (range.fromOffset, range.untilOffset))
          KafkaUtils.commitOffsets(topic, offsets, cfg)
        }
      } else {
        topics.foreach { props =>
          val topic = props._1
          val format = formatName(topics(topic))
          val cfg = new kafka.consumer.ConsumerConfig(SparkKafkaUtils.consumerConfig(format, properties))
          val start = Offsets.stringToNumber(props._2.get("start"), OffsetRequest.EarliestTime)
          val stop = Offsets.stringToNumber(props._2.get("stop"), OffsetRequest.LatestTime)
          val offsets = Offsets.offsetRange(topic, start, stop, cfg)
            .flatMap(o => Map(o._1.partition -> (o._2._1, o._2._2)))
          KafkaUtils.commitOffsets(topic, offsets, cfg)
        }
      }
    }
  }

  /**
    * Converts an RDD of type S to a dataframe of the same type.
    *
    * @param rdd
    */
  override def toDF(rdd: RDD[KMMD]): DataFrame = rdd.toDF
}

object KafkaSource {

  import hydra.spark.configs._

  import scala.collection.JavaConverters._

  def apply(cfg: Config): KafkaSource = {
    val tc = cfg.getObject("topics")

    val keyColumn = cfg.get[String]("keyColumn")

    val topicsMap: Map[String, Map[String, AnyRef]] = Map(tc.entrySet().asScala.map {
      entry =>
        val map = buildTopicMap(entry.getValue().asInstanceOf[ConfigObject])
        entry.getKey -> map
    }.toSeq: _*)

    val properties = cfg.get[Map[String, String]]("properties").getOrElse(Map.empty)

    KafkaSource(topicsMap, properties)
  }

  private def buildTopicMap(cfg: ConfigObject): Map[String, String] = {
    import scala.collection.JavaConversions._

    val map: Map[String, String] = cfg.entrySet().map(entry =>
      entry.getKey -> entry.getValue.unwrapped().toString)(collection.breakOut)

    map
  }

}

