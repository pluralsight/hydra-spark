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

import hydra.spark.api.{Invalid, Valid}
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.apache.spark.sql.SQLContext
import org.scalatest.{FunSpecLike, Matchers}


/**
  * Created by alexsilva on 8/12/16.
  */
class SelectColumnsSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("When selecting columns") {
    it("Should create a new dataframe containing only the selected columns") {
      val sqlContext = new SQLContext(sc)
      val selCols = SelectColumns(Seq("email"))
      val df = selCols.transform(StaticJsonSource.createDF(sqlContext))
      selCols.validate shouldBe Valid
      df.columns.size shouldBe 1
      df.first().get(0) shouldBe "alex-silva@ps.com"
    }

    it("Is invalid with empty lists") {
      SelectColumns(Seq.empty).validate shouldBe a[Invalid]
    }

    it("Should be parseable") {
      val dsl =
        s"""
           dispatch {
           |  version = 1
           |  source {
           |    "hydra.spark.testutils.ListSource" {
           |       messages = ["test","test"]
           |    }
           |  }
           |  operations {
           |    select-columns {
           |      columns = ["col1","col2"]
           |  }
           |}
           |}
    """.stripMargin


      val s= TypesafeDSLParser().parse(dsl).operations.steps.head.asInstanceOf[SelectColumns]
      s.columns shouldBe Seq("col1","col2")
    }
  }
}
