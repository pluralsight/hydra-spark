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

package hydra.spark.operations.io

import hydra.spark.app.parser.TypesafeDSLParser
import org.scalatest._

/**
  * Created by alexsilva on 8/9/16.
  */
class PublishToWebhookSpec extends Matchers with FunSpecLike {
  describe("When publishing to a webhook") {
    it("materializes from a DSL") {
      val dsl =
        """
          |{
          |		"version": 1,
          |		"spark.master": "local[*]",
          |		"name": "test-dispatch",
          |		"source": {
          |			"hydra.spark.testutils.ListSource": {
          |				"messages": ["1", "2", "3"]
          |			}
          |		},
          |		"operations": {
          |			"publish-to-webhook": {
          |				"url": "test.com"
          |			}
          |		}
          |}
        """.stripMargin

      val publish = TypesafeDSLParser().parse(dsl).get.operations.head.asInstanceOf[PublishToWebhook]
      publish.url shouldBe "test.com"
    }
  }
}
