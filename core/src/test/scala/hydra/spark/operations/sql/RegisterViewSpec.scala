package hydra.spark.operations.sql

import hydra.spark.api.Invalid
import hydra.spark.dispatch.SparkDispatch
import hydra.spark.testutils.SharedSparkContext
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

/**
  * Created by alexsilva on 5/3/17.
  */
class RegisterViewSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with SharedSparkContext {

  describe("When registering a view") {
    it("Should be invalid if no name is supplied") {
      val validation = RegisterView("").validate.asInstanceOf[Invalid]
      validation.errors shouldBe Invalid.unapply(validation).get
    }

    it("Should parse and register a local view") {
      val dsl =
        """
          |{
          |    "transport": {
          |        "name": "test",
          |        "version": "1",
          |        "spark.master":"local[*]",
          |        "source": {
          |            "hydra.spark.testutils.ListSource":{
          |             "messages":["1","2","3"]
          |            }
          |        },
          |        "operations": {
          |           "register-view":{
          |               "name":"test_view",
          |               "global":false
          |           }
          |        }
          |    }
          |}
          |
    """.stripMargin


      val d = SparkDispatch(dsl)
      d.run()

      val ctx = ss.sqlContext
      val df = ctx.sql("select * from test_view")
      df.count() shouldBe 3
    }

    it("Should parse and register a global view") {
      val dsl =
        """
          |{
          |    "transport": {
          |        "name": "test",
          |        "version": "1",
          |        "spark.master":"local[*]",
          |        "source": {
          |            "hydra.spark.testutils.ListSource":{
          |             "messages":["1","2","3"]
          |            }
          |        },
          |        "operations": {
          |           "register-view":{
          |               "name":"test_view_global",
          |               "global":true
          |           }
          |        }
          |    }
          |}
          |
    """.stripMargin


      val d = SparkDispatch(dsl)
      d.run()
      val df = ss.sql("select * from  global_temp.test_view_global")
      df.count() shouldBe 3
    }
  }
}