package hydra.spark.operations.sql

import hydra.spark.testutils.{ListOperation, SharedSparkContext, StaticJsonSource}
import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}
import com.google.common.base.CaseFormat._
import com.typesafe.config.ConfigFactory
import hydra.spark.dispatch.SparkBatchTransformation
import spray.json._

class ChangeCaseSpec extends Matchers with FunSpecLike with BeforeAndAfterEach with SharedSparkContext {

  val config = ConfigFactory.parseString(
    """
      |spark.master = "local[4]"
      |spark.ui.enabled = false
      |spark.driver.allowMultipleContexts = false
    """.stripMargin
  )

  describe("When changing case of column names") {
    it("changes case from lower underscore to lower camel") {
      val cc = ChangeCase(LOWER_UNDERSCORE, LOWER_CAMEL)
      val json = StaticJsonSource.msgs(0).parseJson
      SparkBatchTransformation("underscore_camel_test", StaticJsonSource, Seq(cc, ListOperation), config).run()
      val result = ListOperation.l.map(_.parseJson)
      val r2 = result.head.asJsObject.getFields("msgNo") shouldEqual json.asJsObject.getFields("msg_no")

    }
  }
}
