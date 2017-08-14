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

package hydra.spark.operations.transform

import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 8/12/16.
  */
class DropSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("The drop operation") {
    it("drops a column") {
      val sqlContext = ss.sqlContext
      val df = Drop(Seq("data")).transform(StaticJsonSource.createDF(ss))
      df.first().getLong(1) shouldBe 0
    }

    it("is parseable") {
      val dsl =
        """
          |{
          |        "version": 1,
          |        "spark.master": "local[*]",
          |        "name": "test-dispatch",
          |        "source": {
          |            "hydra.spark.testutils.ListSource": {
          |                "messages": ["1", "2", "3"]
          |            }
          |        },
          |        "operations": {
          |          "drop": {
          |           "columns": ["col1", "col2"]
          |           }
          |        }
          |}
        """.stripMargin

      val dispatch = TypesafeDSLParser().parse(dsl).get
      val d = dispatch.operations.head.asInstanceOf[Drop]
      d.columns shouldBe Seq("col1", "col2")
    }
  }
}
