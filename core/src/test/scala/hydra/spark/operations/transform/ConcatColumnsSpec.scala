package hydra.spark.operations.transform

import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.apache.spark.sql.{SQLContext}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by dustinvannoy on 1/20/17.
  */
class ConcatColumnsSpec extends Matchers with FunSpecLike with SharedSparkContext {

  describe("It should concatenate two columns") {
    it("Should allow use of two dataframe columns to make one column") {
      val sqlContext = new SQLContext(sc)
      // ConcatColumns accepts a sequence but dsl will actually send the subtype list
      val df1 = ConcatColumns(List("email","msg-no"), "newColumn").transform(StaticJsonSource.createDF(sqlContext))
      df1.first().getString(3) shouldBe "alex-silva@ps.com|0"
    }
    it("Should be parseable") {
       val dsl =
      """
      |{
        |    "transport": {
          |        "version": 1,
          |        "spark.master": "local[*]",
          |        "name": "test-dispatch",
          |        "source": {
          |            "hydra.spark.testutils.ListSource": {
          |                "messages": ["1", "2", "3"]
          |            }
          |        },
          |        "operations": {
          |          "concat-columns": {
          |           "newName": "newCol",
          |           "columnNames": ["col1", "col2"]
          |           }
          |        }
          |    }
        |}
      """.stripMargin

      val dispatch = TypesafeDSLParser().parse(dsl)
      val d = dispatch.operations.steps.head.asInstanceOf[ConcatColumns]
      d.columnNames shouldBe Seq("col1","col2")
      d.newName shouldBe "newCol"
    }

  }
}
