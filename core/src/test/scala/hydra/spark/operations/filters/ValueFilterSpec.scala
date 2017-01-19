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

package hydra.spark.operations.filters

import com.typesafe.config.ConfigFactory
import hydra.spark.api.Operations
import hydra.spark.dispatch.SparkBatchDispatch
import hydra.spark.testutils.{ ListOperation, SharedSparkContext, StaticJsonSource }
import org.scalatest.{ BeforeAndAfterEach, FunSpecLike, Matchers }
import spray.json._

/**
 * Created by alexsilva on 6/24/16.
 */
class ValueFilterSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with SharedSparkContext {

  val config = ConfigFactory.parseString(
    """
      |spark.master = "local[4]"
      |spark.ui.enabled = false
      |spark.driver.allowMultipleContexts = false
    """.stripMargin
  )

  val t = ValueFilter("msg-no", 0)

  describe("When Filtering by a value") {
    it("Should only include matching rows") {
      val json = StaticJsonSource.msgs(0).parseJson
      SparkBatchDispatch("test", StaticJsonSource, Operations(Seq(t, ListOperation)), config, scl).run()
      ListOperation.l.size shouldBe 1
      ListOperation.l.map(_.parseJson) should contain(json)
    }
  }

  override def beforeEach() = {
    ListOperation.reset()
    super.beforeEach()
  }
}