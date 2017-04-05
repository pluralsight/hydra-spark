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
import hydra.spark.operations.jdbc.{ColumnMapping, DatabaseUpsert}
import hydra.spark.testutils.SharedSparkContext
import hydra.spark.util.DataTypes._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest._
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future

/**
  * Created by alexsilva on 6/18/16.
  */
class DatabaseUpsertSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterAll with BeforeAndAfterEach with H2Spec with Inside with SharedSparkContext {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(1, Seconds))

  val table = "TEST_TABLE"
  val inferredTable = "INFERRED_TEST_TABLE"

  val json = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": 123 } }"""

  override def beforeEach(): Unit = {
    super.beforeEach()
    val f: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10))"))
    val f2: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $inferredTable (user_id integer,user_handle varchar(100)," +
      s"context_ip varchar(10))"))

    eventually(f.value.get.get shouldBe 0)
    eventually(f2.value.get.get shouldBe 0)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE $table"))
    val f2: Future[Int] = database.run(basicUpdate(s"DROP TABLE $inferredTable"))
    eventually(f.value.get.get shouldBe 0)
    eventually(f2.value.get.get shouldBe 0)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    val f: Future[Int] = database.run(basicUpdate(s"DROP ALL OBJECTS"))
    eventually(f.value.get.get shouldBe 0)
  }

  describe("The DatabaseUpsert Transform") {

    it("Should create consistent ids") {

      val mappings = List(ColumnMapping("context.ip", "ip_address", "string"))
      val props = Map("url" -> h2Url)
      val dbu = DatabaseUpsert("table", props, None, mappings)
      val idString = "table" +
        props.map(k => k._1 + "->" + k._2).mkString + "None" + mappings.map(_.toString).mkString
      val digest = MessageDigest.getInstance("MD5")
      val tid = digest.digest(idString.getBytes).map("%02x".format(_)).mkString
      dbu.id shouldEqual tid
    }

    it("Should perform validation") {
      val mappings = List(ColumnMapping("context.ip", "ip_address", "string"))
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

      val mappings = stringTypes.map(name => ColumnMapping("source", "target", name))
      mappings.map(_.`type`) shouldEqual expectedTypes
    }

    it("Should create the correct DF") {
      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("table", Map("url" -> "url"), Some(ColumnMapping("user.id", "user_id", "int")),
        mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      val ndf = dbUpsert.mapping.targetDF(df)

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
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url),
        Some(ColumnMapping("user.id", "user_id", "int")), mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }
    }

    it("Should create the table without PK") {

      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val props = Map("savemode" -> "overwrite", "url" -> url)

      val dbUpsert = DatabaseUpsert("NEW_TABLE", props, None, mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      dbUpsert.transform(df)

      whenReady(database.run(sql"SELECT SQL FROM INFORMATION_SCHEMA.CONSTRAINTS WHERE TABLE_NAME = 'NEW_TABLE'".as[String])) { r =>
        r shouldBe empty
      }
      whenReady(database.run(sql"select * from NEW_TABLE".as[(String, String)])) { r =>
        r shouldBe Seq(("127.0.0.1", "alex"))
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }

    it("Should create the table with PK") {
      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )
      val idCol = Some(ColumnMapping("user.id", "user_id", "int"))

      val props = Map("savemode" -> "overwrite", "url" -> url)

      val dbUpsert = DatabaseUpsert("NEW_TABLE", props, idCol, mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      dbUpsert.transform(df)

      whenReady(database.run(sql"SELECT SQL FROM INFORMATION_SCHEMA.CONSTRAINTS WHERE TABLE_NAME = 'NEW_TABLE'".as[String])) { r =>
        r.toString should include("PRIMARY KEY(USER_ID)")
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }

    it("Should upsert with a PK") {

      import slick.driver.H2Driver.api._

      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert(table, Map("url" -> url),
        Some(ColumnMapping("user.id", "user_id", "int")), mappings)

      val df = ss.sqlContext.read.json(sc.parallelize(json :: Nil))

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex_updated", "id": 123 } }"""

      val ndf = ss.sqlContext.read.json(sc.parallelize(njson :: Nil))

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
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert(table, Map("url" -> url),
        Some(ColumnMapping("user.id", "user_id", "int")), mappings)

      val df = ss.sqlContext.read.json(sc.parallelize(sjson :: Nil))

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex_updated", "id": "123" } }"""

      val ndf = ss.sqlContext.read.json(sc.parallelize(njson :: Nil))

      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
      }
    }

    it("Should use all source fields if no columns are specified") {

      val props = Map("url" -> h2Url)
      val mappings = Seq.empty
      val dbu = DatabaseUpsert(inferredTable, props, Some(ColumnMapping("user.id", "user_id", "int")), mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      val ndf = dbu.transform(df)

      ndf.collect().map { r =>
        r.getString(r.fieldIndex("context_ip")) shouldBe "127.0.0.1"
        r.getString(r.fieldIndex("user_handle")) shouldBe "alex"
        r.getLong(r.fieldIndex("user_id")) shouldBe 123
      }

    }

    it("Should create table and insert using all source fields if no columns are specified") {

      import slick.driver.H2Driver.api._

      val json = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": "123" } }"""
      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = Seq.empty

      val props = Map("savemode" -> "overwrite", "url" -> url)

      val dbUpsert = DatabaseUpsert("INFERRED_NEW_TABLE", props, None, mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select user_id, user_handle, context_ip from INFERRED_NEW_TABLE"
        .as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE INFERRED_NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }

    it("Should upsert with a PK using source fields if no columns specified") {
      import slick.driver.H2Driver.api._
      val sjson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": 123 } }"""
      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"

      val mappings = Seq.empty

      val dbUpsert = DatabaseUpsert(inferredTable, Map("url" -> url),
        Some(ColumnMapping("user_id", "user_id", "int")), mappings)

      val df = ss.sqlContext.read.json(sc.parallelize(sjson :: Nil))

      dbUpsert.transform(df)

      whenReady(database.run(sql"select user_id, user_handle, context_ip from INFERRED_TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context_ip": "127.0.0.1", "user_handle": "alex_updated", "user_id": 123 }"""
      val ndf = ss.sqlContext.read.json(sc.parallelize(njson :: Nil))

      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select user_id, user_handle, context_ip from INFERRED_TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
      }
    }

    it("Should parse without any columns") {

      val dsl =
        """
          |{
          |  "transport": {
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

    it("does not fail with an empty dataset") {
      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert(table, Map("url" -> "url"), Some(ColumnMapping("user.id", "user_id", "int")),
        mappings)

      val rdd = sc.parallelize("" :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      val ndf = dbUpsert.transform(df)

      ndf.collect() shouldBe empty
    }

    it("does not fail with null values") {
      import slick.driver.H2Driver.api._
      val url = s"jdbc:h2:mem:$dbname;DB_CLOSE_DELAY=-1"
      val json = """{ "context": { "ip": null }, "user": { "handle": "alex", "id": null } }"""

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert(table, Map("url" -> url),
        Some(ColumnMapping("user.id", "user_id", "int")), mappings)

      val rdd = sc.parallelize(json :: Nil)

      val df = ss.sqlContext.read.json(rdd)

      dbUpsert.transform(df)

      // works with nulls but converts null Int to 0
      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((0, "alex", null))
      }
    }
  }

}

