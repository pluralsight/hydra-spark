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

    it("Should parse and register a view") {
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
          |               "name":"test_view"
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
  }
}