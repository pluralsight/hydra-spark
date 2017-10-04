package hydra.spark.api


import com.yammer.metrics.Metrics
import com.yammer.metrics.core.MetricName
import org.apache.commons.lang3.ClassUtils
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

class HydraMetrics(sc: SparkContext) extends Serializable {
  type Callback = Unit => Unit

  private val counters = new mutable.HashMap[MetricName, (LongAccumulator, Callback)]

  private val operationMetrics = new mutable.HashMap[String, mutable.Set[OperationMetrics]]()
    with mutable.MultiMap[String, OperationMetrics]

  def getCounters(clazz: Class[_]): Map[String, LongAccumulator] = {
    counters.filterKeys(k=>k.getGroup==clazz.getPackage.getName && k.getType == ClassUtils.getShortClassName(clazz))
      .map(kv => kv._1.getName -> kv._2._1).toMap
  }

  private def doCreateCounter(metricName: MetricName): (LongAccumulator, Callback) = {
    val counter = Metrics.newCounter(metricName)
    val accumulator = sc.longAccumulator(metricName.toString)
    val callback: Callback = _ => counter.inc(accumulator.value)
    (accumulator, callback)
  }

  def createCounter(metricName: String): Unit = {
    val m = new MetricName(getClass, metricName)
    counters.put(m, doCreateCounter(m))
  }

  def addOperationMetric(op: DFOperation, metrics: OperationMetrics) = {
    operationMetrics.addBinding(op.id, metrics)
  }


  def getOperationMetrics(op: DFOperation): Seq[OperationMetrics] = {
    operationMetrics.getOrElse(op.id, Set.empty).toSeq
  }

  def getOrCreateCounter(clazz: Class[_], metricName: String): LongAccumulator = {
    val m = new MetricName(clazz, metricName)
    counters.getOrElseUpdate(m, doCreateCounter(m))._1
  }

  def getCounter(clazz: Class[_], metricName: String): LongAccumulator = {
    counters(new MetricName(clazz, metricName))._1
  }
}

case class OperationMetrics(operationName: String,
                            time: String,
                            recordsWritten: Long,
                            properties: Map[String, String] = Map.empty,
                            counters: Map[String, Long] = Map.empty)