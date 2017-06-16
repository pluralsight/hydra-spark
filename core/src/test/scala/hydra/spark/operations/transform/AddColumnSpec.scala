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

import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 8/12/16.
  */
class AddColumnSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("It should add  a column") {
    it("Should add a column") {
      val sqlContext =  ss.sqlContext
      val df = AddColumn("newColumn", 10).transform(StaticJsonSource.createDF(ss))
      df.first().getInt(3) shouldBe 10

      val df1 = AddColumn("newColumn", "new").transform(StaticJsonSource.createDF(ss))
      df1.first().getString(3) shouldBe "new"
    }

    it("Should add a dynamic column") {
      val sqlContext =  ss.sqlContext
      val df = AddColumn("newColumn", "${10+20}").transform(StaticJsonSource.createDF(ss))
      df.first().getInt(3) shouldBe 30

      val df1 = AddColumn("newColumn", "${T(java.lang.String).valueOf(1)}")
        .transform(StaticJsonSource.createDF(ss))
      df1.first().getString(3) shouldBe "1"
    }

  }
}
