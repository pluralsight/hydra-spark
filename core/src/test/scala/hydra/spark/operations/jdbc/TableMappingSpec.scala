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
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest._

/**
  * Created by alexsilva on 6/18/16.
  */
class TableMappingSpec extends Matchers with FunSpecLike with SharedSparkContext {

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

  describe("when using TableMapping") {
    it("returns a flat df if no cols are specified") {
      val mapping = TableMapping(None, Nil)
      val rdd = sc.parallelize(json :: Nil)
      val tdf = mapping.targetDF(SQLContext.getOrCreate(sc).read.json(rdd))
      val fdf = SQLContext.getOrCreate(sc).read.json(sc.parallelize(flattenedJson :: Nil))
      fdf.schema shouldBe tdf.schema
      fdf.first() shouldBe tdf.first()
    }

    it("returns a flat df if only the id is specified") {
      val mapping = TableMapping(Some(ColumnMapping("user.id", "user_id", LongType)), Nil)
      val rdd = sc.parallelize(json :: Nil)
      val tdf = mapping.targetDF(SQLContext.getOrCreate(sc).read.json(rdd))
      val fdf = SQLContext.getOrCreate(sc).read.json(sc.parallelize(flattenedJson :: Nil))
      fdf.schema shouldBe tdf.schema
      fdf.first() shouldBe tdf.first()
    }

    it("returns a flat df and allows id col to be renamed") {
      val renamedIdJson =
        """
          |{
          |	"context_ip": "127.0.0.1",
          |	"user_handle": "alex",
          |	"user_names_first": "alex",
          |	"user_names_last": "silva",
          |	"userId": 123
          |}
        """.stripMargin
      val mapping = TableMapping(Some(ColumnMapping("user.id", "userId", LongType)), Nil)
      val rdd = sc.parallelize(json :: Nil)
      val tdf = mapping.targetDF(SQLContext.getOrCreate(sc).read.json(rdd))
      val fdf = SQLContext.getOrCreate(sc).read.json(sc.parallelize(renamedIdJson :: Nil))
      fdf.schema.fields should contain theSameElementsAs(tdf.schema.fields)
      fdf.first().toSeq should contain theSameElementsAs(tdf.first().toSeq)
    }

    it("selects only the columns specified in the mapping") {
      val mapping = TableMapping(None,
        Seq(ColumnMapping("context.ip", "ip", StringType),
          ColumnMapping("user.names.first", "firstName", StringType)))
      val rdd = sc.parallelize(json :: Nil)
      val df = SQLContext.getOrCreate(sc).read.json(rdd)
      val tdf = mapping.targetDF(df)
      val schema = StructType(Seq(StructField("ip",StringType),StructField("firstName",StringType)))
      tdf.schema shouldBe schema
      tdf.first() shouldBe df.select("context.ip","user.names.first").first()
    }

    it("selects only the columns specified in the mapping with an id") {
      val mapping = TableMapping(Some(ColumnMapping("user.id", "userId", LongType)),
        Seq(ColumnMapping("context.ip", "ip", StringType),
          ColumnMapping("user.names.first", "firstName", StringType)))
      val rdd = sc.parallelize(json :: Nil)
      val df = SQLContext.getOrCreate(sc).read.json(rdd)
      val tdf = mapping.targetDF(df)
      val schema = StructType(Seq(
        StructField("userId",LongType),StructField("ip",StringType),StructField("firstName",StringType)))
      tdf.schema shouldBe schema
      tdf.first() shouldBe df.select("user.id","context.ip","user.names.first").first()
    }
  }
}

