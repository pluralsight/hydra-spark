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

import java.util.Properties

import com.typesafe.config.{ ConfigFactory, ConfigObject }
import hydra.spark.util.SimpleConsumerConfig
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{ Broker, KafkaUtils, OffsetRange }

import scala.reflect.ClassTag

/**
 * Created by alexsilva on 12/13/16.
 */
object SparkKafkaUtils {

  import hydra.spark.configs._
  import hydra.spark.util.Collections._

  import scala.collection.JavaConverters._

  val config = ConfigFactory.load("reference")

  def createRDD[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R: ClassTag](
    ctx: SparkContext,
    topic: String,
    topicProps: Map[String, Any],
    properties: Map[String, String],
    format: String
  ): RDD[KafkaMessageAndMetadata[K, V]] = {

    val handler = (mmd: MessageAndMetadata[K, V]) =>
      KafkaMessageAndMetadata(mmd.key, mmd.message, mmd.topic, mmd.partition, mmd.offset)
    val cfg = SimpleConsumerConfig(consumerConfig(format, properties))
    val start = Offsets.stringToNumber(topicProps.get("start"), OffsetRequest.EarliestTime)
    val stop = Offsets.stringToNumber(topicProps.get("stop"), OffsetRequest.LatestTime)
    val offsets = Offsets.offsetRange(topic, start, stop, cfg).map(o => OffsetRange(o._1, o._2._1, o._2._2)).toArray
    val brokers = Map.empty[TopicAndPartition, Broker]
    KafkaUtils.createRDD[K, V, KD, VD, KafkaMessageAndMetadata[K, V]](ctx, properties, offsets, brokers, handler)
  }

  def createDStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag, R <: KafkaMessageAndMetadata[K, V]: ClassTag](ctx: StreamingContext, topic: String, topicProps: Map[String, Any], properties: Map[String, String]): DStream[KafkaMessageAndMetadata[K, V]] = {

    val handler = (mmd: MessageAndMetadata[K, V]) => KafkaMessageAndMetadata(mmd.key, mmd.message, mmd.topic, mmd
      .partition, mmd.offset)

    val cfg = SimpleConsumerConfig(consumerConfig("avro", properties))
    val start = Offsets.stringToNumber(topicProps.get("start"), OffsetRequest.EarliestTime)
    val startOffsets = hydra.spark.util.KafkaUtils.getStartOffsets(topic, start, cfg)
    KafkaUtils.createDirectStream[K, V, KD, VD, KafkaMessageAndMetadata[K, V]](ctx, properties, startOffsets, handler)
  }

  def consumerConfig(topicFormat: String, overrideProps: Map[String, String]): Properties = {
    val props = config.getObject("hydra.kafka.formats").entrySet.asScala.filter(c => c.getKey == topicFormat)
    require(props.size == 1)
    val cfgObj = props.head.getValue.asInstanceOf[ConfigObject].toConfig
    val kafkaCfg = Map(cfgObj.entrySet.asScala.toSeq.map(k => k.getKey -> k.getValue.unwrapped.toString): _*)
    val baseKafkaConfig = config.get[Map[String, String]]("kafka.consumer").getOrElse(Map.empty)
    val kafkaConfig: Map[String, AnyRef] = baseKafkaConfig ++ kafkaCfg ++ overrideProps
    kafkaConfig
  }

}
