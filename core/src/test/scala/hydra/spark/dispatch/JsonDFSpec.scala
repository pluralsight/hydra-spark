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

import hydra.spark.api.Operations
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource, StreamingTestDispatch}
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import spray.json._

/**
  * Created by alexsilva on 6/18/16.
  */
class JsonDFSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterAll with SharedSparkContext {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  val source = StaticJsonSource

  var ldf: Option[DataFrame] = None
  val add100 = (df: DataFrame) => {
    val ndf = df.withColumn("msg_no", df("msg_no") + 100)
    ldf = Some(ndf)
    ndf
  }

  val t = Operations(add100, "add100")

  var disp: StreamingTestDispatch[String] = _

  override def beforeAll() = {
    super.beforeAll()
    disp = StreamingTestDispatch(StaticJsonSource, t)

  }

  describe("When translating an RDD to a JSON DF") {

    it("Should call the json implicit type class") {

      val msgNos = StaticJsonSource.msgs.map(_.parseJson.asJsObject.fields("msg_no").toString.toLong + 100)

      disp.run()
      eventually {
        val msgs = ldf.get.take(11).map(c => c.getLong(c.fieldIndex("msg_no")))
        msgs shouldBe msgNos
      }

      disp.dispatch.asInstanceOf[SparkStreamingDispatch[_]].ssc.awaitTerminationOrTimeout(2000)
      disp.stop()
    }
  }

  override def afterAll() = {
    super.afterAll()
    disp.stop()
  }
}
