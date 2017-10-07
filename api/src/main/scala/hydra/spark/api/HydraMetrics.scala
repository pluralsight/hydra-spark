package hydra.spark.api


import org.apache.spark.groupon.metrics.{SparkCounter, UserMetricsSystem}
import org.apache.spark.{SparkContext, SparkEnv}

import scala.collection.mutable

object HydraMetrics {

  private var namespace: String = _

  /**
    * Initialize the metrics system.
    *
    * Must be invoked in the driver before the SparkContext is started.
    *
    */
  def initialize(sc: SparkContext, namespace: String): Unit = {
    UserMetricsSystem.initialize(sc, namespace)
    this.namespace = namespace
  }

  private val countersMap = new mutable.HashMap[String, SparkCounter]

  def counter(source: String, name: String): SparkCounter = {
    val key = s"$source::$name"
    countersMap.getOrElseUpdate(key, UserMetricsSystem.counter(key))
  }

  private[spark] def counterValue(sourceName: String, counterName: String): Option[Long] = {
    SparkEnv.get.metricsSystem.getSourcesByName(sourceName)
      .find(_.metricRegistry.getCounters().get(counterName) != null)
      .map(s => s.metricRegistry.counter(counterName).getCount)
  }

  private val operationMetrics = new mutable.HashMap[String, mutable.Set[OperationMetrics]]()
    with mutable.MultiMap[String, OperationMetrics]

  def getCounters(namespace: String): Map[String, SparkCounter] = {
    countersMap.filterKeys(k => k.startsWith(namespace)).toMap
  }

  def addOperationMetric(name: String, metrics: OperationMetrics) = {
    operationMetrics.addBinding(name, metrics)
  }

  def getOperationMetrics(name: String): Seq[OperationMetrics] = {
    operationMetrics.getOrElse(name, Set.empty).toSeq
  }
}

case class OperationMetrics(operationName: String,
                            time: String,
                            recordsWritten: Long,
                            properties: Map[String, String] = Map.empty,
                            counters: Map[String, Long] = Map.empty)