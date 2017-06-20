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
import hydra.spark.api.{ Invalid, Operations }
import hydra.spark.dispatch.SparkBatchDispatch
import hydra.spark.testutils.{ ListOperation, SharedSparkContext, StaticJsonSource }
import org.scalatest.{ BeforeAndAfterEach, FunSpecLike, Matchers }

/**
 * Created by alexsilva on 6/24/16.
 */
class RegexFilterSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with SharedSparkContext {

  val config = ConfigFactory.parseString(
    """
      |spark.master = "local[4]"
      |spark.ui.enabled = false
      |spark.driver.allowMultipleContexts = false
    """.stripMargin
  )

  val t = RegexFilter("email", ".*@.*")

  describe("When Filtering by a value") {
    it("Should be invalid if missing field") {
      val validation = RegexFilter("", ".*@.*").validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }

    it("Should be invalid if missing regex") {
      val validation = RegexFilter("a", "").validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }

    it("Should accept regex") {
      val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(Seq(t, ListOperation)), config)
      sd.run()
      ListOperation.l.size shouldBe 6
    }
  }

  override def beforeEach = {
    ListOperation.reset()
  }
}