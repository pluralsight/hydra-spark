package hydra.spark.api

import org.apache.spark.SparkContext

class HydraContext(sc: SparkContext) {

  val metrics = new HydraMetrics(sc)

}