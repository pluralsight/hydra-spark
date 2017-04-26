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

package hydra.spark.sources

import hydra.spark.api._
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.operations.jdbc.H2Spec
import hydra.spark.testutils.SharedSparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 6/2/16.
  */
class JdbcSourceSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterAll with H2Spec with SharedSparkContext {

  import TestImplicits._

  implicit override val patienceConfig = PatienceConfig(timeout = Span(12, Seconds), interval = Span(1, Seconds))

  var sctx: StreamingContext = _

  var ctx: SQLContext = _

  val table = "TEST_TABLE"

  override def beforeAll() = {

    import slick.driver.H2Driver.api._

    super.beforeAll()

    val f = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10))"))

    sctx = new StreamingContext(sc, org.apache.spark.streaming.Seconds(1))
    ctx = SparkSession.builder().getOrCreate().sqlContext

    eventually(f.value.get.get shouldBe 0)

    for (i <- 0 to 10) {
      whenReady(database.run(basicUpdate(s"""INSERT INTO $table VALUES ($i,'user$i','ip$i')"""))) { r =>
        r shouldBe Seq((1))
      }
    }

    whenReady(database.run(sql"select count(*) from TEST_TABLE".as[(Int)])) { r =>
      r shouldBe Seq((11))
    }
  }

  override def afterAll() = {
    super.afterAll()
    val f = database.run(basicUpdate(s"DROP TABLE $table"))
    sctx.stop(false, true)
    eventually(f.value.get.get shouldBe 0)
  }

  describe("When using jdbc sources") {

    it("Should not allow streaming") {
      intercept[InvalidDslException] {
        new JdbcSource("table", Map.empty).createStream(sctx)
      }
    }

    it("Should be invalid if no table is specified") {
      val source = new JdbcSource("", Map.empty)
      val validation = source.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }

    it("Should be invalid if no url is specified") {
      val source = new JdbcSource(table, Map.empty)
      val validation = source.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }

    it("Should create an RDD from a table") {
      val source = new JdbcSource(table, Map("url" -> h2Url))
      source.validate shouldBe Valid
      val rows = source.createDF(ctx)
      rows.map(r => r.getInt(0)).collect() should contain allOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    }

    it("Should create an RDD from a query") {
      val query = s"(select * from $table where user_id<=5 order by user_id asc) as testquery"
      val source = new JdbcSource(query, Map("url" -> h2Url))
      source.validate shouldBe Valid
      val rows = source.createDF(ctx)
      rows.map(r => r.getInt(0)).collect() shouldBe Array(0, 1, 2, 3, 4, 5)
    }

    it("should return the correct RDD type") {
      val dsl =
        """
     {
          |  "transport": {
          |    "version": 1,
          |    "spark.master":"local[*]",
          |    "interval":"1s",
          |
          |    "source": {
          |      "jdbc": {
          |        "dbtable":"test",
          |          "properties": {
          |            "group.id": "user-sign-in-groupx",
          |            "message.max.bytes": 10000,
          |            "metadata.broker.list": "localhost:6667"
          |          }
          |      }
          |    },
          |      "operations": {
          |        "database-upsert": {
          |          "table": "rabbit.user_sign_in_export",
          |              "properties": {
          |                "url": "jdbc:postgresql://lovlhost/prod",
          |                "user": "test",
          |                "password":"test"
          |              }
          |        }
          |        }
          |      }
          |}
        """.stripMargin

      val rdd = ctx.read.json(sc.parallelize(Seq("""{"name":"alex"}"""))).rdd
      val disp = TypesafeDSLParser().parse(dsl)
      disp.source.asInstanceOf[Source[Row]].toDF(rdd).count() shouldBe 1
    }
  }
}

