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

package hydra.spark.dispatch

/**
 * Created by alexsilva on 6/21/16.
 */

import com.typesafe.config.ConfigFactory
import hydra.spark.api._
import hydra.spark.testutils.SharedSparkContext
import hydra.spark.util.{RDDConversions, StreamingContextLike}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSpecLike, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
 * Created by alexsilva on 6/20/16.
 */
class SparkStreamingDispatchTest extends Matchers with FunSpecLike with MockFactory with SharedSparkContext {

  val props = Map("batchDuration" -> "5s", "spark.local.dir" -> "/tmp/hydra", "spark.master" -> "local[*]")

  var ssc: ContextLike = _

  override def beforeAll() = {
    super.beforeAll()
    ssc = StreamingContextLike(new StreamingContext(sc, Seconds(1)))
  }

  describe("When Creating a Dispatch using Spark") {
    it("Be configured properly") {

      val c = ConfigFactory.parseMap(props.asJava)
      val sd = SparkStreamingDispatch("test", EmptySource, Operations(Seq.empty), c, ssc)
      val conf = sd.ctx.sparkContext.getConf

    }
  }

  override def afterAll() = {
    super.afterAll()
    ssc.stop()
  }
}

object EmptySource extends Source[String] {
  override def name: String = "empty-source"

  override def createStream(sc: StreamingContext): DStream[String] = ???

  override def createDF(ctx: SQLContext): DataFrame = ???

  override def validate: ValidationResult = Valid

  override def toDF(rdd: RDD[String]): DataFrame = RDDConversions.StringDF.toDF(rdd)
}
