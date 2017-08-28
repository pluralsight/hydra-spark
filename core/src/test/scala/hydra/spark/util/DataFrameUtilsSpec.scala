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
      val ctx = ss
      import ctx.implicits._
      val df = ctx.sqlContext.read.json(ctx.createDataset(Seq(json)))
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
      val ndf = ctx.sqlContext.read.json(ctx.createDataset(Seq(dropJson)))
      ddf.schema.fields.map(_.name) shouldBe ndf.schema.fields.map(_.name)
      ddf.first() shouldBe ndf.first()
    }

    it("drops a root-level column") {
      val ctx = ss
      import ctx.implicits._
      val df = ctx.sqlContext.read.json(ctx.createDataset(Seq(json)))
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
      val ndf = ctx.sqlContext.read.json(ctx.createDataset(Seq(dropJson)))
      ddf.schema shouldBe ndf.schema
      ddf.first() shouldBe ndf.first()
    }
  }
}
