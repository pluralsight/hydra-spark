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

import com.typesafe.config.{ConfigFactory, ConfigObject}
import kafka.api.OffsetRequest
import kafka.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies, OffsetRange}

import scala.reflect.ClassTag

/**
  * Created by alexsilva on 12/13/16.
  */
object SparkKafkaUtils {

  import hydra.spark.configs._
  import hydra.spark.util.Collections._

  import scala.collection.JavaConverters._

  val config = ConfigFactory.load("reference")

  def createRDD[K: ClassTag, V: ClassTag]
  (ctx: SparkContext, topic: String, topicProps: Map[String, Any], properties: Map[String, String], format: String): RDD[ConsumerRecord[K, V]] = {

    val cfg = new ConsumerConfig(consumerConfig(format, properties))
    val start = Offsets.stringToNumber(topicProps.get("start"), OffsetRequest.EarliestTime)
    val stop = Offsets.stringToNumber(topicProps.get("stop"), OffsetRequest.LatestTime)
    val offsets: Array[OffsetRange] = Offsets.offsetRange(topic, start, stop, cfg).map(o => OffsetRange(o._1, o._2._1, o._2._2)).toArray
    val kafkaParams: Map[String, Object] = (topicProps ++ properties).map(v => v._1 -> v._2.asInstanceOf[AnyRef])
    KafkaUtils.createRDD[K, V](ctx, kafkaParams.asJava, offsets, LocationStrategies.PreferConsistent)
  }

  def createDStream[K: ClassTag, V: ClassTag](ctx: StreamingContext, topic: String, topicProps: Map[String, Any],
                                              properties: Map[String, String]): DStream[ConsumerRecord[K, V]] = {

    val cfg = new ConsumerConfig(consumerConfig("avro", properties))
    val start = Offsets.stringToNumber(topicProps.get("start"), OffsetRequest.EarliestTime)
    val startOffsets = hydra.spark.util.KafkaUtils.getStartOffsets(topic, start, cfg)
    val kafkaParams: Map[String, Object] = (topicProps ++ properties).map(v => v._1 -> v._2.asInstanceOf[AnyRef])

    val stream = KafkaUtils.createDirectStream[K, V](
      ctx,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[K, V](Seq(topic), kafkaParams)
    )

    stream
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
