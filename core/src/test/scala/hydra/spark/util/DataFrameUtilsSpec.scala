package hydra.spark.util

import hydra.spark.testutils.SharedSparkContext
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/11/17.
  */
class DataFrameUtilsSpec extends Matchers with FunSpecLike with SharedSparkContext {

  import DataFrameUtils._

  val json =
    """
      |{
      |	"id":1,
      |  "context": {
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


  describe("When using DataFrameUtils") {
    it("drops a nested column") {
      val df = ss.sqlContext.read.json(sc.parallelize(json :: Nil))
      val dropJson =
        """
          |{
          |	"id":1,
          | "context": {
          |		"ip": "127.0.0.1"
          |	},
          |	"user": {
          |		"names": {
          |			"first": "alex",
          |			"last": "silva"
          |		},
          |		"id": 123
          |	}
          |}
        """.stripMargin

      val ddf = df.dropNestedColumn("user.handle")
      val ndf = ss.sqlContext.read.json(sc.parallelize(dropJson :: Nil))
      ddf.schema.fields.map(_.name) shouldBe ndf.schema.fields.map(_.name)
      ddf.first() shouldBe ndf.first()
    }

    it("drops a root-level column") {
      val df = ss.sqlContext.read.json(sc.parallelize(json :: Nil))
      val dropJson =
        """
          |{
          |  "context": {
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
      val ddf = df.dropNestedColumn("id")
      val ndf = ss.sqlContext.read.json(sc.parallelize(dropJson :: Nil))
      ddf.schema shouldBe ndf.schema
      ddf.first() shouldBe ndf.first()
    }
  }
}
