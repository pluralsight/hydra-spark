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

import hydra.spark.testutils.{ SharedSparkContext, StaticJsonSource }
import org.apache.spark.sql.SQLContext
import org.scalatest.{ FunSpecLike, Matchers }

/**
 * Created by alexsilva on 8/12/16.
 */
class RenameColumnSpec extends Matchers with FunSpecLike with SharedSparkContext {

  var sqlContext: SQLContext = _

  override def beforeAll() = {
    super.beforeAll()
    sqlContext =  ss.sqlContext
  }

  describe("It should rename a column") {
    it("Should rename a column") {
      val df = RenameColumn("msg_no", "message-number").transform(StaticJsonSource.createDF(ss))
      df.show
      val row = df.first()
      intercept[IllegalArgumentException] {
        row.fieldIndex("msg_no")
      }
      row.fieldIndex("message-number") shouldBe 2
    }
    it("Skip if column doesn't exist") {
      val df = RenameColumn("msgno", "message-number").transform(StaticJsonSource.createDF(ss))
      val row = df.first()
      row.fieldIndex("msg_no") should be > 0
    }

  }
}
