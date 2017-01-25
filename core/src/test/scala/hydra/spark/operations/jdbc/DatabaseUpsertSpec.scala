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

package hydra.spark.dsl.jdbc

import java.security.MessageDigest

import hydra.spark.api.{Invalid, Valid}
import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.operations.jdbc.{DatabaseUpsert, SQLMapping}
import hydra.spark.util.DataTypes._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future

/**
  * Created by alexsilva on 6/18/16.
  */
class DatabaseUpsertSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterAll with BeforeAndAfterEach with H2Spec with Inside {

  val sparkConf = new SparkConf()
    .setMaster("local[6]")
    .setAppName("hydra")
    .set("spark.ui.enabled", "false")
    .set("spark.local.dir", "/tmp")

  var ctx: Option[SparkContext] = None

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  val table = "TEST_TABLE"

  val json = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": 123 } }"""

  override def beforeEach(): Unit = {
    val f: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10))"))

    eventually(f.value.get.get shouldBe 0)

    ctx = Some(SparkContext.getOrCreate(sparkConf))
  }

  override def afterEach(): Unit = {
    ctx.foreach(_.stop())
    val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE $table"))
    eventually(f.value.get.get shouldBe 0)
  }

  override def afterAll(): Unit = {
    val f: Future[Int] = database.run(basicUpdate(s"DROP ALL OBJECTS"))
    eventually(f.value.get.get shouldBe 0)
  }

  describe("The DatabaseUpsert Transform") {

    it("Should create consistent ids") {

      val mappings = List(SQLMapping("context.ip", "ip_address", "string"))
      val props = Map("url" -> h2Url)
      val dbu = DatabaseUpsert("table", props, None, mappings)
      val idString = "table" +
        props.map(k => k._1 + "->" + k._2).mkString + "None" + mappings.map(_.toString).mkString
      val digest = MessageDigest.getInstance("MD5")
      val tid = digest.digest(idString.getBytes).map("%02x".format(_)).mkString
      dbu.id shouldEqual tid
    }

    it("Should perform validation") {
      val mappings = List(SQLMapping("context.ip", "ip_address", "string"))
      DatabaseUpsert("table", Map("url" -> h2Url), None, mappings).validate shouldBe Valid

      inside(DatabaseUpsert("table", Map.empty, None, Seq.empty).validate) {
        case Invalid(errors) => errors should have size 1
      }
    }
    it("Should convert strings into DataTypes") {

      val stringTypes = Seq("null", "date", "timestamp", "binary", "integer", "int", "boolean", "long",
        "double", "float", "short", "byte", "string", "calendarinterval")

      val expectedTypes = Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, IntegerType,
        BooleanType, LongType, DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType)

      val mappings = stringTypes.map(name => SQLMapping("source", "target", name))
      mappings.map(_.`type`) shouldEqual expectedTypes
    }

    it("Should create a schema from the mappings") {

      val stringTypes = Seq("int", "boolean", "string")

      val mappings = stringTypes.
        zipWithIndex.map { case (name, i) => SQLMapping(s"source$i", s"target$i", name) }

      val targetSchema = StructField("id", IntegerType) +:
        stringTypes.zipWithIndex.map { case (name, i) => StructField(s"target$i", nameToDataType(name)) }

      val dbUpsert = DatabaseUpsert(
        "table",
        Map("url" -> "url"), Some(SQLMapping("id", "id", "int")), mappings
      )

      dbUpsert.mapping.targetSchema shouldEqual targetSchema
    }

    it("Should create the correct DF") {
      val mappings = List(
        SQLMapping("context.ip", "ip_address", "string"),
        SQLMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("table", Map("url" -> "url"), Some(SQLMapping("user.id", "user_id", "int")),
        mappings)

      val rdd = ctx.get.parallelize(json :: Nil)

      val df = SQLContext.getOrCreate(ctx.get).read.json(rdd)

      val ndf = dbUpsert.prepareDF(df)

      ndf.show()

      val row: Row = ndf.take(1)(0)

      //    row.getStruct(0)(0) shouldBe "127.0.0.1"
      row(0) shouldBe 123
      row(1) shouldBe "127.0.0.1"
      row(2) shouldBe "alex"
    }

    it("Should perform inserts w/o a PK") {

      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        SQLMapping("context.ip", "ip_address", "string"),
        SQLMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url),
        Some(SQLMapping("user.id", "user_id", "int")), mappings)

      val rdd = ctx.get.parallelize(json :: Nil)

      val df = SQLContext.getOrCreate(ctx.get).read.json(rdd)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }
    }

    it("Should create the table") {

      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        SQLMapping("context.ip", "ip_address", "string"),
        SQLMapping("user.handle", "username", "string")
      )

      val props = Map("savemode" -> "overwrite", "url" -> url)

      val dbUpsert = DatabaseUpsert("NEW_TABLE", props, None, mappings)

      val rdd = ctx.get.parallelize(json :: Nil)

      val df = SQLContext.getOrCreate(ctx.get).read.json(rdd)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from NEW_TABLE".as[(String, String)])) { r =>
        r shouldBe Seq(("127.0.0.1", "alex"))
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }

    it("Should upsert with a PK") {

      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        SQLMapping("context.ip", "ip_address", "string"),
        SQLMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url),
        Some(SQLMapping("user.id", "user_id", "int")), mappings)

      val df = SQLContext.getOrCreate(ctx.get).read.json(ctx.get.parallelize(json :: Nil))

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex_updated", "id": 123 } }"""

      val ndf = SQLContext.getOrCreate(ctx.get).read.json(ctx.get.parallelize(njson :: Nil))

      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
      }
    }

    it("Should upsert with a string PK") {
      val sjson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": "123" } }"""

      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        SQLMapping("context.ip", "ip_address", "string"),
        SQLMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url),
        Some(SQLMapping("user.id", "user_id", "int")), mappings)

      val df = SQLContext.getOrCreate(ctx.get).read.json(ctx.get.parallelize(sjson :: Nil))

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex_updated", "id": "123" } }"""

      val ndf = SQLContext.getOrCreate(ctx.get).read.json(ctx.get.parallelize(njson :: Nil))

      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
      }
    }

    it("Should use nulls for unknown columns") {

      val mappings = List(SQLMapping("context.ip_address", "ip_address", "string"))
      val props = Map("url" -> h2Url)
      val dbu = DatabaseUpsert("table", props, None, mappings)

      val rdd = ctx.get.parallelize(json :: Nil)

      val df = SQLContext.getOrCreate(ctx.get).read.json(rdd)

      val ndf = dbu.transform(df)

      ndf.show()

      ndf.collect.foreach(_.getString(0) shouldBe null)

    }

    it("Should use all source fields as strings if no columns are specified") {

      val f = database.run(basicUpdate(s"CREATE TABLE test2 (user_id integer,user_handle varchar(100)," +
        s"context_ip varchar(10))"))

      eventually(f.value.get.get shouldBe 0)

      val props = Map("url" -> h2Url)
      val dbu = DatabaseUpsert("test2", props, None, Seq.empty)

      val rdd = ctx.get.parallelize(json :: Nil)

      val df = SQLContext.getOrCreate(ctx.get).read.json(rdd)

      val ndf = dbu.transform(df)

      ndf.show()

      ndf.collect().map { r =>
        r.getString(r.fieldIndex("context_ip")) shouldBe "127.0.0.1"
        r.getString(r.fieldIndex("user_handle")) shouldBe "alex"
        r.getLong(r.fieldIndex("user_id")) shouldBe 123L
      }

    }

    ignore("Should deserialize a json column as a string by default") {

      val seg =
        """
          |{
          |	"anonymousId": "cdec8853-a1ad-42e0-a992-e582c50cb8c7",
          |	"channel": "client",
          |	"context": {
          |		"ip": "81.196.139.164",
          |		"library": {
          |			"name": "analytics.js",
          |			"version": "3.0.0"
          |		},
          |		"page": {
          |			"path": "/library/",
          |			"referrer": "https://www.pluralsight.com/",
          |			"search": "",
          |			"title": "Dashboard | Pluralsight",
          |			"url": "https://app.pluralsight.com/library/"
          |		},
          |		"userAgent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
          |	},
          |	"integrations": {},
          |	"messageId": "ajs-f4ba867f146a6f398aa4c16177121018",
          |	"originalTimestamp": "2016-08-02T16:12:35.360Z",
          |	"projectId": "mp7Mf0chBn",
          |	"properties": {
          |		"path": "/library/",
          |		"referrer": "https://www.pluralsight.com/",
          |		"search": "",
          |		"title": "Dashboard | Pluralsight",
          |		"url": "https://app.pluralsight.com/library/"
          |	},
          |	"receivedAt": "2016-08-02T16:12:36.231Z",
          |	"sentAt": "2016-08-02T16:12:35.368Z",
          |	"timestamp": "2016-08-02T16:12:36.223Z",
          |	"type": "page",
          |	"userId": "8baca5c0-3585-4a5d-96cf-41e73a66e769",
          |	"version": 2
          |}
        """.stripMargin

      val mappings = List(SQLMapping("context.page", "ip_address", "json"))

      val props = Map("url" -> h2Url)
      val dbu =

        DatabaseUpsert(
          "TEST_TABLE",
          props, None, mappings
        )

      val rdd = ctx.get.parallelize(seg :: Nil)

      val df = SQLContext.getOrCreate(ctx.get).read.json(rdd)

      val ndf = dbu.transform(df)

      ndf.show()

    }

    it("Should parse without any columns") {

      val dsl =
        """
          |{
          |  "dispatch": {
          |    "version": 1,
          |    "interval": "20s"
          |
          |  "source": {
          |    "kafka": {
          |      "topics": {
          |        "test": {
          |          "format": "avro",
          |          "start": "largest"
          |        }
          |      },
          |      "properties": {
          |        "schema.registry.url": "http://localhost",
          |        "metadata.broker.list": "localhost:6667"
          |      }
          |    }
          |  },
          |  "operations": {
          |    "database-upsert": {
          |      "table": "test_streaming",
          |      "properties": {
          |        "url": "jdbc:postgresql://localhost/prod",
          |        "driver": "org.postgresql.Driver",
          |        "user": "test",
          |        "password": "test"
          |      }
          |    }
          |  }
          |  }
          |}
        """.stripMargin

      val dispatch = TypesafeDSLParser().parse(dsl)
      dispatch.operations.steps.head.validate shouldBe Valid
    }
  }

}

