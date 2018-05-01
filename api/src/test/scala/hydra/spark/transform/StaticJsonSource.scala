package hydra.spark.transform

import hydra.spark.api.{Source, Valid}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoders, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
 * Created by alexsilva on 1/3/17.
 */
object StaticJsonSource extends Source[String] {

  val email = """"email":"hydra@dataisawesome.com","""
  val msgs = for (i <- 0 to 10)
    yield s"""{"msg_no": $i, ${if (i % 2 == 0) email else ""}
         | "data": {"value": "hello no $i", "time": ${System.currentTimeMillis}}
      }""".stripMargin

  override def createStream(sc: StreamingContext): DStream[String] = {
    val rdd = sc.sparkContext.parallelize(msgs, 1)
    val lines = mutable.Queue[RDD[String]](rdd)
    sc.queueStream[String](lines, false, rdd)
  }

  override def validate = Valid

  override def createDF(ctx: SparkSession): DataFrame = {
    import ctx.implicits._
    ctx.read.json(ctx.createDataset(msgs))
  }

  override def toDF(rdd: RDD[String]): DataFrame = {
    val spark: SQLContext = SparkSession.builder().getOrCreate.sqlContext
    val ds = spark.createDataset(rdd)(Encoders.STRING)
    ds.toDF()
  }
}
