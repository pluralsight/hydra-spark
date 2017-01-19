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

package hydra.spark.operations

/**
  * Created by alexsilva on 1/3/17.
  */

import com.typesafe.config.ConfigFactory
import hydra.spark.api.{Invalid, Operations, Valid}
import hydra.spark.dispatch.SparkBatchDispatch
import hydra.spark.test.utils.{SharedSparkContext, StaticJsonSource}
import hydra.spark.util.DataTypes._
import org.apache.spark.sql.types.StringType
import org.scalatest._
import org.scalatest.concurrent.Eventually

/**
  * Created by alexsilva on 8/9/16.
  */
class CassandraWriterSpec extends Matchers with FunSpecLike with Inside with BeforeAndAfterAll with Eventually
  with BeforeAndAfterEach with SharedSparkContext {

  var sparkD: Option[SparkBatchDispatch[String]] = None

  val sparkProps = ConfigFactory.parseString(
    s"""
       |spark.master = "local[1]"
       |spark.default.parallelism	= 1
       |spark.ui.enabled = false
       |spark.driver.allowMultipleContexts = false
       |spark.cassandra.connection.host = "localhost"
       |spark.cassandra.connection.port = "9142"
       |spark.cassandra.auth.username = cassandra
       |spark.cassandra.auth.password = cassandra
        """.stripMargin
  )

  override def afterEach() = {
    // EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    sparkD.foreach(_.stop())
  }

  override def beforeAll() = {
    //  EmbeddedCassandraServerHelper.startEmbeddedCassandra()//"cassandra.yaml", 20000L)
    //  val dataLoader = new DataLoader("hydra", "localhost:9142")
    //  dataLoader.load(new ClassPathYamlDataSet("hydra-keysace.yaml"))
  }

  describe("When writing to Cassandra") {
    it("Should be invalid without a tablespace and/or a table") {

      var c = CassandraWriter("", "table", List.empty)
      var validation = c.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get

      c = CassandraWriter("keyspace", "", List.empty)
      validation = c.validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get

      c = CassandraWriter("keyspace", "table", List.empty)
      c.validate shouldBe Valid
    }

    it("Should be created from a config") {

      val props = ConfigFactory.parseString(
        s"""
           |{
           |   "keyspace" : "testks",
           |   "table" : "test-table",
           |    "columns": [{
           |				"source": "msg-no",
           |				"name": "msg_no",
           |				"type": "string"
           |			}, {
           |				"source": "data.email",
           |				"name": "email"
           |			}],
           |   "properties": {
           |      "spark.cassandra.connection.host" : "localhost:7777",
           |      "spark.cassandra.auth.username": "cassandra",
           |      "spark.cassandra.auth.password": "cassandra"
           |   }
           |}
        """.stripMargin
      )

      val c = CassandraWriter(props)
      c.validate shouldBe Valid
      c.keyspace shouldBe "testks"
      c.table shouldBe "test-table"
      c.columns.map(c => (c.name, c.source, c.`type`)) shouldBe List(
        ("msg_no", "msg-no", StringType),
        ("email", "data.email", StringType)
      )
    }

    ignore("Should write to Cassandra") {
      val cols = List(
        CassandraColumnMapping("msg-no", "msgNo", "string"),
        CassandraColumnMapping("data.email", "email", "string")
      )
      val c = CassandraWriter("hydra", "hydra-spark", cols)
      c.validate shouldBe Valid
      c.keyspace shouldBe "hydra"
      c.table shouldBe "hydra-spark"

      val sd = SparkBatchDispatch("test", StaticJsonSource, Operations(c), sparkProps, scl)
      sparkD = Some(sd)
      //sd.run
    }
  }
}
