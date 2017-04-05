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

package hydra.spark.util

import java.net.ConnectException

import hydra.spark.internal.Logging
import kafka.api._
import kafka.common.{ErrorMapping, OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, SimpleConsumer}
import kafka.network.BlockingChannel
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.errors.{BrokerNotAvailableException, LeaderNotAvailableException, NotLeaderForPartitionException}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.util.Random
import scala.util.control.NonFatal

/**
  * Created by alexsilva on 1/2/16.
  */
object KafkaUtils extends Logging {
  def simpleConsumer(broker: Broker, cfg: ConsumerConfig): SimpleConsumer =
    new SimpleConsumer(broker.host, broker.port, 5000,
      BlockingChannel.UseDefaultBufferSize, cfg.groupId)

  // new SimpleConsumer(broker.host, broker.port, cfg.socketTimeoutMs, cfg.socketReceiveBufferBytes, cfg.clientId)

  def brokerList(cfg: ConsumerConfig): List[Broker] =
    cfg.props.getString(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).split(",").toList.map(Broker.apply)

  def partitionLeaders(topic: String, brokers: Iterable[Broker],
                       cfg: ConsumerConfig): Map[Int, Option[Broker]] = {
    val it = Random.shuffle(brokers).take(5).iterator.flatMap { broker =>
      val consumer = simpleConsumer(broker, cfg)
      try {
        Some(partitionLeaders(topic, consumer))
      } catch {
        case e: Exception =>
          log.error("connection failed for broker {}", broker)
          log.error("Using consumer {}", consumer)
          log.error(e.getMessage, e)
          None
      } finally {
        consumer.close()
      }
    }
    if (it.hasNext) it.next() else throw new BrokerNotAvailableException("operation failed for all brokers")
  }

  private def partitionLeaders(topic: String, consumer: SimpleConsumer): Map[Int, Option[Broker]] = {
    val topicMeta = consumer.send(new TopicMetadataRequest(Seq(topic), 0)).topicsMetadata.head
    ErrorMapping.maybeThrowException(topicMeta.errorCode)
    topicMeta.partitionsMetadata.map { partitionMeta =>
      ErrorMapping.maybeThrowException(partitionMeta.errorCode)
      (partitionMeta.partitionId, partitionMeta.leader.map { b => Broker(b.host, b.port) })
    }.toMap
  }

  def topicCoordinator(topic: String, broker: Broker, config: ConsumerConfig,
                       versionId: Short = GroupCoordinatorRequest.CurrentVersion): Option[Broker] = {
    val consumer = simpleConsumer(broker, config)
    try {
      val coord = consumer.send(new GroupCoordinatorRequest(config.groupId, versionId, 0, consumer.clientId)).coordinatorOpt
      coord match {
        case Some(c) => Some(Broker(c.host, c.port))
        case None => None
      }
    } catch {
      case e: ConnectException =>
        log.warn("connection failed for broker {}", broker)
        None
    } finally {
      consumer.close()
    }
  }

  def partitionOffset(tap: TopicPartition, time: Long, consumer: SimpleConsumer): Long = {
    val taap = TopicAndPartition(tap.topic(), tap.partition())
    val pof = consumer.getOffsetsBefore(OffsetRequest(Map(taap ->
      PartitionOffsetRequestInfo(time, 100)))).partitionErrorAndOffsets(taap)
    ErrorMapping.maybeThrowException(pof.error)
    pof.offsets.head
  }

  def partitionOffsets(topic: String, time: Long, leaders: Map[Int, Option[Broker]], config: ConsumerConfig): Map[Int, Long] =
    leaders.par.map {
      case (partition, None) =>
        throw new LeaderNotAvailableException(s"no leader for partition ${partition}")
      case (partition, Some(leader)) =>
        val consumer = simpleConsumer(leader, config)
        try {
          (partition, partitionOffset(new TopicPartition(topic, partition), time, consumer))
        } finally {
          consumer.close()
        }
    }.seq

  def fetchOffsets(topic: String, coordinators: Map[Int, Option[Broker]], leaders: Map[Int, Option[Broker]],
                   config: ConsumerConfig): Map[Int, Long] =
    coordinators.par.map {
      case (partition, None) =>
        throw new LeaderNotAvailableException(s"no leader for partition ${partition}")
      case (partition, Some(coordinator)) =>
        val consumer = simpleConsumer(coordinator, config)
        try {
          (partition, fetchOffset(new TopicPartition(topic, partition), config.groupId, consumer,
            leaders(partition).get, config))
        } finally {
          consumer.close()
        }
    }.seq

  def offsetRange(topic: String, startTime: Long, stopTime: Long,
                  config: ConsumerConfig): Map[TopicPartition, (Long, Long)] = {
    val brokers = brokerList(config)

    val (startOffsets, stopOffsets) = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      (partitionOffsets(topic, startTime, leaders, config), partitionOffsets(topic, stopTime, leaders, config))
    }, config)

    startOffsets.map {
      case (partition, startOffset) => (new TopicPartition(topic, partition), (startOffset, stopOffsets(partition)))
    }
  }

  def getStartOffsets(topic: String, startTime: Long, config: ConsumerConfig): Map[TopicPartition, Long] = {
    val brokers = brokerList(config)

    val startOffsets = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, config)
      partitionOffsets(topic, startTime, leaders, config)
    }, config)

    startOffsets.map {
      case (partition, startOffset) => (new TopicPartition(topic, partition), startOffset)
    }
  }

  def lastGroupOffsets(topic: String, cfg: ConsumerConfig, stopOffset: Long = OffsetRequest.LatestTime):
  Map[TopicPartition, (Long, Long)] = {
    val brokers = brokerList(cfg)
    val anyBroker = Random.shuffle(brokers).head
    val (startOffsets, stopOffsets) = retryIfNoLeader({
      val leaders = partitionLeaders(topic, brokers, cfg)

      //There is only one coordinator for a topic
      val coordinator: Option[Broker] = topicCoordinator(topic, anyBroker, cfg)

      //we then build a "coordinators" map using partition information in the leaders map
      val coordinatorMap: Map[Int, Option[Broker]] = leaders.map(e => (e._1, coordinator))

      fetchOffsets(topic, coordinatorMap, leaders, cfg) -> partitionOffsets(topic, stopOffset, leaders, cfg)
    }, cfg)

    val offsets = startOffsets.map {
      case (partition, startOffset) => (new TopicPartition(topic, partition), (startOffset, stopOffsets(partition)))
    }

    offsets
  }

  private def fetchOffset(top: TopicPartition, groupId: String, consumer: SimpleConsumer,
                          leader: Broker, config: ConsumerConfig): Long = {
    val fetchRequest = OffsetFetchRequest(
      groupId = groupId,
      requestInfo = Seq(TopicAndPartition(top.topic(), top.partition())),
      versionId = 1
    )
    val fetchResponse = consumer.fetchOffsets(fetchRequest)
    ErrorMapping.maybeThrowException(fetchResponse.requestInfo.head._2.error)

    var offset = fetchResponse.requestInfo.head._2.offset

    /**
      * According to Kafka docs:
      * Note that if there is no offset associated with a topic-partition under that consumer group the broker
      * does not set an error code (since it is not really an error),
      * but returns empty metadata and sets the offset field to -1.
      */
    if (offset == -1) {
      //get latest offset.  That's what we need the leader for the partition here as well, and not the coordinator
      val lconsumer = simpleConsumer(leader, config)
      try {
        offset = partitionOffset(top, OffsetRequest.EarliestTime, lconsumer)
      } finally {
        lconsumer.close
      }
    }
    offset

  }

  def commitOffsets(topic: String, offsets: Map[Int, (Long, Long)], config: ConsumerConfig): Boolean = {
    val brokers = brokerList(config)
    val anyBroker = Random.shuffle(brokers).head
    val coordinator: Option[Broker] = topicCoordinator(topic, anyBroker, config)
    val offsetsToComit = offsets.map {
      case (p, v) => (new TopicPartition(topic, p), v._2)
    }
    require(coordinator.isDefined, new KafkaException(s"No coordinator found for topic $topic"))
    commitOffsets(offsetsToComit, coordinator.get, config)
  }

  private def commitOffsets(offsets: Map[TopicPartition, Long], coordinator: Broker,
                            config: ConsumerConfig): Boolean = {
    val offsetsMetadata = offsets.map { case (k, v) =>
      (TopicAndPartition(k.topic(), k.partition()), OffsetAndMetadata(v))
    }
    val commitRequest = OffsetCommitRequest(
      groupId = config.groupId,
      requestInfo = offsetsMetadata,
      versionId = 1,
      correlationId = 0,
      clientId = config.clientId
    )
    try {
      val commitResponse = simpleConsumer(coordinator, config).commitOffsets(commitRequest)
      if (commitResponse.hasError) {
        ErrorMapping.maybeThrowException(commitResponse.commitStatus.find(_._2 != ErrorMapping.NoError).head._2)
        false
      }
      true
    } catch {
      case NonFatal(e) => {
        log.error("Unable to commit offsets.", e)
        false
      }
    }
  }

  def retryIfNoLeader[E](e: => E, config: ConsumerConfig): E = {
    def sleep() {
      log.warn("sleeping for {} ms", config.refreshLeaderBackoffMs)
      Thread.sleep(config.refreshLeaderBackoffMs)
    }

    def attempt(e: => E, nr: Int = 1): E = if (nr < 5) {
      try (e) catch {
        case ex: LeaderNotAvailableException =>
          sleep(); attempt(e, nr + 1)
        case ex: NotLeaderForPartitionException =>
          sleep(); attempt(e, nr + 1)
        case ex: ConnectException => sleep(); attempt(e, nr + 1)
      }
    } else e

    attempt(e)
  }

  def instantiateDecoder[M <: Decoder[_]](cls: String, props: VerifiableProperties): M = {
    import scala.reflect.runtime.universe._
    import scala.reflect.runtime.{currentMirror => cm}
    val cl = cm.classSymbol(Class.forName(cls))
    val clazz = cm.reflectClass(cl)
    val vp = typeOf[VerifiableProperties]
    //todo: once we can ditch 2.10 (thanks Spark), we can switch this back to .decl
    val ctors: List[MethodSymbol] = cl.toType.decl(reflect.runtime.universe.termNames.CONSTRUCTOR)
      .asTerm.alternatives.map {
      _.asMethod
    }

    val ctor = ctors.find(x => x.paramLists(0)(0).typeSignature == vp)

    ctor match {
      case Some(c) => {
        val ctorm = clazz.reflectConstructor(c)
        val obj = ctorm(props).asInstanceOf[M]
        obj
      }
      case None => None.asInstanceOf[M]
    }
  }
}

case class Broker(host: String, port: Int) {
  override def toString: String = host + ":" + port
}

object Broker {
  def apply(s: String): Broker = s.split(":") match {
    case Array(host) => Broker(host, 9092)
    case Array(host, port) => Broker(host, port.toInt)
  }

}

