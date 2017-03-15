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

import hydra.spark.operations.jdbc.{ColumnMapping, DatabaseUpsert}
import hydra.spark.testutils.SharedSparkContext
import hydra.spark.util.DataTypes._
import org.apache.spark.sql.SQLContext
import org.scalatest.concurrent.{Eventually, PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.Future

/**
  * Created by alexsilva on 6/18/16.
  */
class DatabaseUpsertPostgresSpec extends Matchers with FunSpecLike with ScalaFutures with PatienceConfiguration
  with Eventually with BeforeAndAfterEach with PostgresSpec with SharedSparkContext {

  implicit override val patienceConfig = PatienceConfig(timeout = Span(2, Seconds), interval = Span(1, Seconds))

  val table = "TEST_TABLE"

  val json = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex", "id": 123 } }"""

  override def afterEach(): Unit = {
    super.afterEach()
    val f: Future[Int] = database.run(basicUpdate(s"DROP TABLE $table"))
    eventually(f.value.get.get shouldBe 0)
  }

  describe("The DatabaseUpsert Should work with Postgres") {


  it("Should perform inserts w/o a PK") {
    import slick.driver.H2Driver.api._

    val f: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10))"))

    eventually(f.value.get.get shouldBe 0)

    val mappings = List(
      ColumnMapping("context.ip", "ip_address", "string"),
      ColumnMapping("user.handle", "username", "string")
    )

    val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url), None, mappings)

    val rdd = sc.parallelize(json :: Nil)

    val df = SQLContext.getOrCreate(sc).read.json(rdd)

    dbUpsert.transform(df)

    whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
      r shouldBe Seq((0, "alex", "127.0.0.1"))
    }
  }

  it("Should upsert with a PK and infer the other columns") {

    val f: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10),primary key(user_id))"))

    eventually(f.value.get.get shouldBe 0)

    val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url),
      Some(ColumnMapping("user.id", "user_id", "long")), Seq.empty)


  }

  it("Should upsert with a PK") {

    import slick.driver.H2Driver.api._

    val f: Future[Int] = database.run(basicUpdate(s"CREATE TABLE $table (user_id integer,username varchar(100)," +
      s"ip_address varchar(10),primary key(user_id))"))

    eventually(f.value.get.get shouldBe 0)

    val mappings = List(
      ColumnMapping("context.ip", "ip_address", "string"),
      ColumnMapping("user.handle", "username", "string")
    )

    val dbUpsert = DatabaseUpsert("TEST_TABLE", Map("url" -> url),
      Some(ColumnMapping("user.id", "user_id", "long")), mappings)

    val df = SQLContext.getOrCreate(sc).read.json(sc.parallelize(json :: Nil))

    dbUpsert.transform(df)

    whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
      r shouldBe Seq((123, "alex", "127.0.0.1"))
    }

    val njson = """{ "context": { "ip": "127.0.0.1" }, "user": { "handle": "alex_updated", "id": 123 } }"""

    val ndf = SQLContext.getOrCreate(sc).read.json(sc.parallelize(njson :: Nil))

    dbUpsert.transform(ndf)

    whenReady(database.run(sql"select * from TEST_TABLE".as[(Int, String, String)])) { r =>
      r shouldBe Seq((123, "alex_updated", "127.0.0.1"))
    }
  }
  }
}

