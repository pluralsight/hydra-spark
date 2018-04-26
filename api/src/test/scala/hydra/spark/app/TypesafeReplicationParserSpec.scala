package hydra.spark.app.parser

import hydra.spark.api.InvalidDslException
import org.scalatest.{FlatSpecLike, Matchers}

class TypesafeReplicationParserSpec extends Matchers with FlatSpecLike {
  "The Typesafe replication parser" should "parse a well-formed DSL" in {
    val dsl =
      """
        |replicate {
        |  topicsPattern:"exp.identity.*"
        |  startingOffsets:"earliest"
        |  sink: "jdbc"
        |}
      """.stripMargin

    val r = TypesafeReplicationParser.parse(dsl).get
    r.sink shouldBe "jdbc"
    r.topics shouldBe Right("exp.identity.*")
    r.startingOffsets shouldBe "earliest"
  }

  it should "error if neither topics or topicPattern are present" in {
    val dsl =
      """
        |{
        |   replicate {
        |     startingOffsets:"earliest"
        |     sink: "jdbc"
        |  }
        |}
      """.stripMargin

    intercept[InvalidDslException] {
      TypesafeReplicationParser.parse(dsl).get
    }
  }

  it should "error if both topics and topicPattern are present" in {
    val dsl =
      """
        |{
          |  replicate {
         |    topics:"topic1,topic2"
          |  topicsPattern:"exp.identity.*"
          |  startingOffsets:"earliest"
          |  sink: "jdbc"
        |  }
        |}
      """.stripMargin

    intercept[InvalidDslException] {
      TypesafeReplicationParser.parse(dsl).get
    }
  }
}
