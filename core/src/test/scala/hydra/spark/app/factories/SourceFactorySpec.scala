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

package hydra.spark.app.factories

/**
 * Created by alexsilva on 1/3/17.
 */

import com.typesafe.config.ConfigFactory
import hydra.spark.api.InvalidDslException
import hydra.spark.testutils.EmptySource
import org.scalatest.{ FunSpecLike, Matchers }

/**
 * Created by alexsilva on 6/2/16.
 */
class SourceFactorySpec extends Matchers with FunSpecLike {

  val cfg = ConfigFactory.parseString(
    """
      |"empty": {
      | "testName" : "alex-test"
      |		}
    """.stripMargin
  )

  val testSource = EmptySource("alex-test")

  describe("When using a classpath dsl factory") {

    it("reads sources in a package") {
      val fact = ClasspathDslElementFactory(Seq("hydra.spark.testutils"), Seq.empty)
      fact.createSource(cfg.root, ConfigFactory.empty()) shouldBe testSource
    }

    it("errors with the wrong package") {
      val fact = ClasspathDslElementFactory(Seq("hydra.spark.testutilss"), Seq.empty)
      intercept[InvalidDslException] {
        fact.createSource(cfg.root, ConfigFactory.empty()) shouldBe testSource
      }
    }
  }
}

