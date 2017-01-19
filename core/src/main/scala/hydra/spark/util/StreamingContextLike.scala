package hydra.spark.util

import hydra.spark.api.{ContextLike, DispatchDetails}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

/**
  * Created by alexsilva on 1/18/17.
  */
case class StreamingContextLike(ssc: StreamingContext) extends ContextLike {
  override def sparkContext: SparkContext = ssc.sparkContext

  override def isValidDispatch(dispatch: DispatchDetails[_]): Boolean = dispatch.isStreaming

  override def stop(): Unit = ssc.stop(true, false)
}
