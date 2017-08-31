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

import hydra.spark.operations.common.ColumnMapping
import hydra.spark.testutils.SharedSparkContext
import hydra.spark.util.DataTypes._
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.Future

/**
  * Created by alexsilva on 6/18/16.
  */
class DatabaseUpsertPostgresSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterEach with PostgresSpec with SharedSparkContext {

  import TestImplicits._
  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(1, Seconds))

  val table = "TEST_TABLE"
  val inferredTable = "INFERRED_TEST_TABLE"

  val json = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": 123 } }"""
  val jsonCaseSensitive = """{ "context": { "userIp": "127.0.0.1" }, "user": { "userName": "alex", "userId": 123 } }"""

  override def beforeEach(): Unit = {
    super.beforeEach()
    val f: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10))"))
    eventually(f.value.get.get shouldBe 0)

    val f2: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $inferredTable (user_id integer, user_handle varchar(100), context_ip varchar(10), primary key(user_id))"))
    eventually(f2.value.get.get shouldBe 0)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE IF EXISTS $table"))
    eventually(f.value.get.get shouldBe 0)

    val f2: Future[Int] = database.run(basicUpdate(s"DROP TABLE IF EXISTS $inferredTable"))
    eventually(f2.value.get.get shouldBe 0)
  }

  describe("The DatabaseUpsert Should work with Postgres") {

    it("Should create the table without PK") {
      import slick.driver.PostgresDriver.api._

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val props = Map("savemode" -> "overwrite")

      val dbUpsert = DatabaseUpsert("NEW_TABLE", url, props, None, mappings)

      val ds = ss.createDataset(Seq(json))

      val df = ss.sqlContext.read.json(ds)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select conname from pg_constraint where conname = 'new_table_pkey'".as[String])) { r =>
        r shouldBe empty
      }
      whenReady(database.run(sql"select * from NEW_TABLE".as[(String, String)])) { r =>
        r shouldBe Seq(("127.0.0.1", "alex"))
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }

    it("Should create the table with PK") {
      import slick.driver.PostgresDriver.api._

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )
      val idCol = Some(ColumnMapping("user.id", "user_id", "int"))

      val props = Map("savemode" -> "overwrite")

      val dbUpsert = DatabaseUpsert("NEW_TABLE", url, props, idCol, mappings)

      val ds = ss.createDataset(Seq(json))

      val df = ss.sqlContext.read.json(ds)

      dbUpsert.aroundPreStart(sc)
      dbUpsert.transform(df)

      whenReady(database.run(sql"select conname from pg_constraint where conname = 'new_table_pkey'".as[String])) { r =>
        r should contain("new_table_pkey")
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }

    it("Should perform inserts w/o a PK") {
      import slick.driver.PostgresDriver.api._

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("TEST_TABLE", url, Map.empty, None, mappings)
      dbUpsert.aroundPreStart(sc)
      val ds = ss.createDataset(Seq(json))

      val df = ss.sqlContext.read.json(ds)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((0, "alex", "127.0.0.1"))
      }
    }

    it("Should upsert with a PK") {
      import slick.driver.PostgresDriver.api._

      val f: Future[Int] = database.run(basicUpdate(s"ALTER TABLE $table ADD CONSTRAINT pk_user_id primary key (user_id)"))

      eventually(f.value.get.get shouldBe 0)

      val mappings = List(
        ColumnMapping("context.ip", "ip_address", "string"),
        ColumnMapping("user.handle", "username", "string")
      )

      val dbUpsert = DatabaseUpsert("TEST_TABLE", url, Map.empty,
        Some(ColumnMapping("user.id", "user_id", "long")), mappings)

      val df = ss.sqlContext.read.json(ss.createDataset(Seq(json)))
      dbUpsert.aroundPreStart(sc)
      dbUpsert.transform(df)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex_updated", "id": 123 } }"""

      val ndf = ss.sqlContext.read.json(ss.createDataset(Seq(njson)))
      dbUpsert.aroundPreStart(sc)
      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
      }
    }

    it("Should upsert with a PK using source fields if no columns specified") {
      import slick.driver.PostgresDriver.api._

      val mappings = Seq.empty

      val dbUpsert = DatabaseUpsert(inferredTable,  url, Map.empty,
        Some(ColumnMapping("user_id", "user_id", "int")), mappings)

      val df = ss.sqlContext.read.json(ss.createDataset(Seq(json)))
      dbUpsert.aroundPreStart(sc)
      dbUpsert.transform(df)

      whenReady(database.run(sql"select user_id, user_handle, context_ip from INFERRED_TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex", "127.0.0.1"))
      }

      val njson = """{ "context_ip": "127.0.0.1", "user_handle": "alex_updated", "user_id": 123 }"""
      val ndf = ss.sqlContext.read.json(ss.createDataset(Seq(njson)))
      dbUpsert.aroundPreStart(sc)
      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select user_id, user_handle, context_ip from INFERRED_TEST_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
      }
    }

    it("Should create with PK and upsert for case sensitive columns") {
      import slick.driver.PostgresDriver.api._

      val mappings = List(
        ColumnMapping("context.userIp", "ipAddress", "string"),
        ColumnMapping("user.userName", "userName", "string")
      )
      val idCol = Some(ColumnMapping("user.userId", "userId", "int"))

      val props = Map("savemode" -> "overwrite")

      val dbUpsert = DatabaseUpsert("NEW_TABLE", url, props, idCol, mappings)
      dbUpsert.aroundPreStart(sc)
      val ds = ss.createDataset(Seq(jsonCaseSensitive))
      ss.sqlContext.setConf("spark.sql.caseSensitive", "true") //not important but want to test as well
      val df = ss.sqlContext.read.json(ds)

      dbUpsert.transform(df)

      whenReady(database.run(sql"select conname from pg_constraint where conname = 'new_table_pkey'".as[String])) { r =>
        r should contain("new_table_pkey")
      }

      val njson = """{ "context": { "userIp": "127.0.0.1" }, "user": { "userName": "alex_updated", "userId": 123 } }"""

      val ndf = ss.sqlContext.read.json(ss.createDataset(Seq(njson)))

      dbUpsert.transform(ndf)

      whenReady(database.run(sql"select * from NEW_TABLE".as[(Int, String, String)])) { r =>
        r shouldBe Seq((123, "127.0.0.1", "alex_updated"))
        val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE NEW_TABLE"))
        eventually(f.value.get.get shouldBe 0)
      }
    }
  }
}

