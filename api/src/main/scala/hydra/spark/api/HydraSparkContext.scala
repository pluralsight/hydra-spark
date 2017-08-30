package hydra.spark.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.HashMap


object HydraSparkContext {

  private val RECORDS_WRITTEN: String = "RECORDS_WRITTEN"

  private val counters = new HashMap[String, LongAccumulator]()

  lazy val sc = SparkSession.builder().getOrCreate().sparkContext

  counters += HydraSparkContext.RECORDS_WRITTEN -> sc.longAccumulator(HydraSparkContext.RECORDS_WRITTEN)

  def getOrCreateCounter(name: String): LongAccumulator = synchronized {
    val counter = counters.get(name).map(x => name -> x).getOrElse(name -> sc.longAccumulator(name))
    counters += counter
    counter._2
  }

  lazy val recordsWritten: LongAccumulator =
    counters(HydraSparkContext.RECORDS_WRITTEN)
}