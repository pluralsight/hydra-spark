package hydra.spark.util

import hydra.spark.testutils.SharedSparkContext
import org.joda.time.DateTime
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/11/17.
  */
class ExpressionsSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("When using Expressions") {
    it("replaces the current timestamp") {
      new DateTime(Expressions.parseExpression("#{current_timestamp()}").toLong)
        .getMonthOfYear shouldBe DateTime.now().getMonthOfYear()
    }
  }
}
