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

package hydra.spark.dsl.parser

/**
  * Created by alexsilva on 1/3/17.
  */

import com.typesafe.config.ConfigFactory
import hydra.spark.operations.common.ColumnMapping
import hydra.spark.operations.filters.{RegexFilter, ValueFilter}
import hydra.spark.operations.io.SaveAsJson
import hydra.spark.operations.jdbc.DatabaseUpsert
import hydra.spark.sources.kafka.KafkaSource
import kafka.api.OffsetRequest
import org.apache.spark.sql.types.{LongType, StringType}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 6/20/16.
  */
class DslParserSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with BeforeAndAfterAll {

  val dsl =
    """
      {
      |  version = 1
      |  name = test-job
      |  streaming.interval = 5s
      |  spark.local.dir = /tmp/hydra
      |  spark.ui.enabled = false
      |  spark.master = "local[*]"
      |
      |source {
      |  kafka {
      |    topics {
      |      segment {
      |        format = json
      |        start = -1
      |      }
      |    }
      |    properties {
      |      schema.registry.url = "http://192.16.100.72:8081"
      |      message.max.bytes = 10000
      |      metadata.broker.list = "localhost:6667"
      |    }
      |  }
      |}
      |operations {
      |  "1:value-filter" {
      |    column = ip
      |    value = alex
      |  }
      |  "3:regex-filter" {
      |    field = ip
      |    expr = alex
      |  }
      |  "2:database-upsert" {
      |    table = youbora
      |    idColumn {
      |      name = id
      |      type = long
      |    }
      |    columns = [{
      |      source = anonymousId
      |      name = user_handle
      |      type = string
      |    },
      |     {
      |      source = context.ip
      |      name = ip_address
      |    }]
      |    properties {
      |      url = "jdbc:postgresql://localhost/youbora"
      |      user = youbora
      |      password = password
      |    }
      |  }
      |}
      |}
      |
    """.stripMargin

  val cfg = ConfigFactory.parseString(dsl)

  describe("When parsing the dsl") {
    it("Should load sources") {
      import configs.syntax._
      val d = TypesafeDSLParser().parse(dsl).get

      d.name shouldBe "test-job"

      d.isStreaming shouldBe true

      val cfg = d.dsl

      cfg.get[FiniteDuration]("streaming.interval").value shouldBe 5.seconds

      cfg.getString("spark.local.dir") shouldBe "/tmp/hydra"

      d.source.name shouldBe "kafka"

      val kafkaSource = d.source.asInstanceOf[KafkaSource]

      val props = Map(
        "schema.registry.url" -> "http://192.16.100.72:8081",
        "message.max.bytes" -> "10000",
        "metadata.broker.list" -> "localhost:6667"
      )
      props.foreach(p => kafkaSource.properties should contain(p))
      kafkaSource.topics.size shouldEqual 1
      kafkaSource.topics shouldEqual Map("segment" -> Map("format" -> "json", "start" -> OffsetRequest.LatestTime.toString))

      val operations = d.operations
      operations.size shouldBe 3

      val e = operations(0)

      e shouldBe a[ValueFilter]

      e.asInstanceOf[ValueFilter].column shouldBe "ip"
      e.asInstanceOf[ValueFilter].value shouldBe "alex"

      val t = operations(1)

      t shouldBe a[DatabaseUpsert]

      val db = t.asInstanceOf[DatabaseUpsert]

      db.properties shouldBe
        Map("url" -> "jdbc:postgresql://localhost/youbora", "user" -> "youbora", "password" -> "password")

      db.idColumn shouldBe Some(ColumnMapping("id", "id", LongType))

      db.columns shouldBe Seq(
        ColumnMapping("anonymousId", "user_handle", StringType),
        ColumnMapping("context.ip", "ip_address", StringType)
      )

      val r = operations(2)
      r shouldBe a[RegexFilter]
      val regex = r.asInstanceOf[RegexFilter]
      regex.expr shouldBe "alex"
      regex.field shouldBe "ip"
    }

    it("Should use the default Hydra properties if they are not present in the DSL") {
      val defDsl =
        """
          |      {
          |      		"version": 1,
          |      		"name": "test-defaults-job",
          |         "spark.master":"local[*]",
          |         "spark.ui.enabled":false,
          |      		"streaming.interval": "5s",
          |      	"source": {
          |      		"kafka": {
          |      			"topics": {
          |      				"test.topic": {
          |      					"format": "json",
          |      					"start": -1
          |      				}
          |      			}
          |      		}
          |      	},
          |      	"operations": {
          |      		"1:value-filter": {
          |      			"column": "ip",
          |      			"value": "alex"
          |      		}
          |      	}
          |      }
        """.
          stripMargin

      val sd = TypesafeDSLParser().parse(defDsl).get
      val source = sd.source.asInstanceOf[KafkaSource]
      source.topics.get("test.topic").get("format") shouldBe "json"
      source.properties.get("metadata.broker.list") shouldBe Some("localhost:6667")
      source.properties.get("schema.registry.url") shouldBe Some("localhost:8080")
    }

    it("Should merge the default Hydra properties if they are partially present in the DSL") {
      val defDsl =
        """
          |{
          |		"version": 1,
          |		"name": "test - defaults - job",
          |		"spark.master": "local[*]",
          |		"spark.ui.enabled": true,
          |		"source": {
          |			"kafka": {
          |				"topics": {
          |					"segment": {
          |						"format": "avro",
          |						"start": -1
          |					}
          |				},
          |				 "properties": {
          |          "group.id": "support",
          |         "metadata.broker.list": "broker1:6667,broker2:6667,broker3:6667",
          |          "schema.registry.url": "http://tester:8081",
          |          "message.max.bytes":"10000"
          |        }
          |			}
          |		},
          |		"operations": {
          |			"value-filter": {
          |				"column": "ip",
          |				"value": "alex"
          |			}
          |		}
          |}
        """.stripMargin

      val sd = TypesafeDSLParser().parse(defDsl).get
      val source = sd.source.asInstanceOf[KafkaSource]

      source.properties("metadata.broker.list") shouldBe "broker1:6667,broker2:6667,broker3:6667"
      source.properties("schema.registry.url") shouldBe "http://tester:8081"
      source.properties("message.max.bytes") shouldBe "10000"
    }
    it("Should parse a batch dispatch") {
      val defDsl =
        """
          |{
          |		"version": 1,
          |		"name": "test-defaults-job",
          |		"spark.master": "local[*]",
          |		"spark.ui.enabled": true,
          |
          |		"source": {
          |			"kafka": {
          |				"topics": {
          |					"segment": {
          |						"format": "avro",
          |						"start": -1
          |					}
          |				},
          |				"properties": {
          |					"schema.registry.url": "http://127.0.01:8081",
          |					"message.max.bytes": 10000
          |				}
          |			}
          |		},
          |		"operations": {
          |			"value-filter": {
          |				"column": "ip",
          |				"value": "alex"
          |			}
          |		}
          |}
        """
          .stripMargin

      val sd = TypesafeDSLParser().parse(defDsl).get
      sd.isStreaming shouldBe false
      val source = sd.source.asInstanceOf[KafkaSource]

      source.properties("metadata.broker.list") shouldBe "localhost:6667"
      source.properties("schema.registry.url") shouldBe "http://127.0.01:8081"
      source.properties("message.max.bytes") shouldBe "10000"
    }

    it("Should substitute variables") {
      //only HOCON is supported in substitution
      val defDsl =
        s"""
           |      		version= 1,
           |      		name = test-defaults-job
           |         spark.master = "local[*]"
           |         json.dir = "/test"
           |         schemaUrl = testschema
           |
           |      	source {
           |      		kafka {
           |      			topics {
           |      				segment {
           |      					format = avro,
           |      					start = -1
           |      				}
           |      			}
           |           properties {
           |             schema.registry.url= $${schemaUrl}
           |          }
           |         }
           |      	},
           |      	operations {
           |      		"hydra.spark.operations.io.SaveAsJson" {
           |      			directory= $${json.dir}
           |      			value= alex
           |      		}
           |      	}
        """.stripMargin
      val sd = TypesafeDSLParser().parse(defDsl).get
      sd.source.asInstanceOf[KafkaSource].properties("schema.registry.url") shouldBe "testschema"
      val op = sd.operations.head.asInstanceOf[SaveAsJson]
      op.directory shouldBe "/test"
    }

    it("Should include an external file") {

      val path = Thread.currentThread().getContextClassLoader.getResource("credentials.conf").getFile
      val defDsl =
        s"""
           | {
           |  include file("$path")
           |  version = 1
           |  name = test-defaults-job
           |  spark.master = "local[*]"
           |  json.dir = "/test"
           |
         |  source {
           |    kafka {
           |      topics {
           |        segment {
           |          format = avro,
           |          start = -1
           |        }
           |      }
           |      properties {
           |        mydb-un = $${credentials.database.username}
           |        mydb-pwd = $${credentials.database.password}
           |      }
           |    }
           |  },
           |  operations {
           |    "hydra.spark.operations.io.SaveAsJson" {
           |      directory = $${json.dir}
           |      value = alex
           |    }
           |  }
           |}
        """.stripMargin
      val sd = TypesafeDSLParser().parse(defDsl).get
      sd.source.asInstanceOf[KafkaSource].properties("mydb-un") shouldBe "test-user"
      sd.source.asInstanceOf[KafkaSource].properties("mydb-pwd") shouldBe "password"

      val op = sd.operations.head.asInstanceOf[SaveAsJson]
      op.directory shouldBe "/test"
    }

  }
}

