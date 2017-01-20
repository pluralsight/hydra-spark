package hydra.spark.operations.transform

import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by dustinvannoy on 1/20/17.
  */
class ConcatColumnsSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("It should concatenate two columns") {
    it("Should allow use of dataframe columns in expressions") {
      val sqlContext = new SQLContext(sc)
      val df = ConcatColumns(Seq("email","msg-no"), "newColumn").transform(StaticJsonSource.createDF(sqlContext))
      df.first().getString(3) shouldBe "alex-silva@ps.com|0"
    }
  }
}
