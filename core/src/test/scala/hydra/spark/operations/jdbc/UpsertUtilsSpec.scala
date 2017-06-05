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

package hydra.spark.operations.jdbc

import hydra.spark.testutils.SharedSparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest._

/**
  * Created by alexsilva on 6/18/16.
  */
class UpsertUtilsSpec extends Matchers with FunSpecLike with SharedSparkContext {

  val json =
    """
      |{
      |	"context": {
      |		"ip": "127.0.0.1"
      |	},
      |	"user": {
      |		"handle": "alex",
      |		"names": {
      |			"first": "alex",
      |			"last": "silva"
      |		},
      |		"id": 123
      |	}
      |}
    """.stripMargin


  describe("when using upsert utils") {
    it ("returns all column paths") {
      val rdd = sc.parallelize(json :: Nil)
      val df =  ss.sqlContext.read.json(rdd)
      val flattened = UpsertUtils.columnPaths(df.schema)
      flattened should contain allOf("context.ip","user.handle", "user.id", "user.names.first", "user.names.last")
    }

    it ("flattens a dataframe") {

      val rdd = sc.parallelize(json :: Nil)
      val df =  ss.sqlContext.read.json(rdd)

      val flattenedJson =
        """
          |{
          |	"context_ip": "127.0.0.1",
          |	"user_handle": "alex",
          |	"user_names_first": "alex",
          |	"user_names_last": "silva",
          |	"user_id": 123
          |}
        """.stripMargin

      val frdd = sc.parallelize(flattenedJson :: Nil)
      val fdf =  ss.sqlContext.read.json(frdd)

      val flat = UpsertUtils.flatten(df,"_")
      flat.schema shouldBe fdf.schema
      fdf.first() shouldBe flat.first()
    }
  }
}

