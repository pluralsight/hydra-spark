package hydra.spark.submit

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 1/18/17.
  */
class DslRunnerSpec extends Matchers with FunSpecLike {

  val validDsl =
    """
    {
 |	"dispatch": {
 |		"version": 1,
 |		"spark.master": "local[*]",
 |		"name": "test-dispatch",
 |		"source": {
 |			"hydra.spark.testutils.ListSource": {
 |        "messages":["1","2","3"]
 |   }
 |   },
 |			"operations": {
 |				"print-rows": {}
 |			}
 |		}
 |	}
    """.stripMargin

  val invalidDsl =
    """
      |{
      |	"dispatch": {
      |		"version": 1,
      |   "spark.master" = "mesos",
      |  "name": "test-dispatch",
      |		"source": {
      |			"kafka": {
      |				"topics": {
      |					"hydra.TestSpark": {
      |						"format": "avro",
      |						"start": "smallest"
      |					}
      |				},
      |				"properties": {
      |					"schema.registry.url": "http://localhost:8081",
      |					"message.max.bytes": 100000,
      |					"group.id":"test",
      |					"metadata.broker.list": "localhost:6667"
      |				}
      |			}
      |		}
      |	}
      |}
    """.stripMargin

  describe("When using DslRunnerSpec") {
    it("invalidates the dsl") {
      intercept[RuntimeException] {
        DslRunner.runJob(invalidDsl)
      }
    }
    it("runs the dsl") {
      DslRunner.runJob(validDsl)
    }
  }

}
