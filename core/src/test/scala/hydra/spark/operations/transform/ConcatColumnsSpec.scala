package hydra.spark.operations.transform

import hydra.spark.dsl.parser.TypesafeDSLParser
import hydra.spark.testutils.{SharedSparkContext, StaticJsonSource}
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by dustinvannoy on 1/20/17.
  */
class ConcatColumnsSpec extends Matchers with FunSpecLike with SharedSparkContext {
  import TestImplicits._

  describe("It should concatenate two columns") {
    it("Should allow use of two dataframe columns to make one column") {
      val sqlContext =  ss.sqlContext
      // ConcatColumns accepts a sequence but dsl will actually send the subtype list
      val df1 = ConcatColumns(List("email","msg_no"), "newColumn").transform(StaticJsonSource.createDF(ss))
      df1.first().getString(3) shouldBe "hydra@dataisawesome.com|0"
    }
    it("Should not break on an empty dataset") {
      // ConcatColumns accepts a sequence but dsl will actually send the subtype list
      val df =  ss.sqlContext.read.json(ss.createDataset(Seq.empty[String]))
      val df1 = ConcatColumns(List("email","msg_no"), "newColumn").transform(df)
      df1.collect() shouldBe empty
    }
    it("Should be parseable") {
       val dsl =
      """
      |{
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
        |}
      """.stripMargin

      val dispatch = TypesafeDSLParser().parse(dsl).get
      val d = dispatch.operations.head.asInstanceOf[ConcatColumns]
      d.columnNames shouldBe Seq("col1","col2")
      d.newName shouldBe "newCol"
    }

  }
}
