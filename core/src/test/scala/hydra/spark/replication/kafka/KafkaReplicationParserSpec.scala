package hydra.spark.replication.kafka

import java.util.UUID

import hydra.spark.api.InvalidDslException
import org.scalatest.{FlatSpecLike, Matchers}

class KafkaReplicationParserSpec extends Matchers with FlatSpecLike {
  "The Typesafe replication parser" should "parse a well-formed DSL" in {
    val dsl =
      """
        |replicate {
        |  name:"test"
        |  topicPattern:"exp.identity.*"
        |  startingOffsets:"earliest"
        |  connection.url="jdbc"
        |}
      """.stripMargin

    val r = KafkaReplicationParser.parse(dsl).get
    r.topics shouldBe Right("exp.identity.*")
    r.startingOffsets shouldBe "earliest"
    r.connectionInfo shouldBe Map("url" -> "jdbc")
    r.saveMode shouldBe "Append"
    r.name shouldBe "test"
  }

  it should "throw an error if `primaryKey` is present" in {
    val dsl =
      """
        |replicate {
        |  name:"test"
        |  topicPattern:"exp.identity.*"
        |  startingOffsets:"earliest"
        |  connection.url="jdbc"
        |  primaryKey = test
        |}
      """.stripMargin

    intercept[IllegalArgumentException] {
      KafkaReplicationParser.parse(dsl).get
    }
  }

  it should "parse multiple primary keys into a map" in {
    val dsl =
      """
        |replicate {
        |  name:"test"
        |  topicPattern:"exp.identity.*"
        |  startingOffsets:"earliest"
        |  saveMode = overwrite
        |  connection.url="jdbc"
        |  primaryKeys {
        |      exp.identity.UserSignedIn=test
        |      exp.identity.UserSignedOut=handle
        |  }
        |}
      """.stripMargin

    val r = KafkaReplicationParser.parse(dsl).get
    r.saveMode shouldBe "Overwrite"
    r.primaryKeys should contain theSameElementsAs Map(
      "exp.identity.UserSignedIn" -> "test",
      "exp.identity.UserSignedOut" -> "handle")
  }

  it should "have an empty map if no primary keys" in {
    val dsl =
      """
        |replicate {
        |  name:"test"
        |  topicPattern:"exp.identity.*"
        |  startingOffsets:"earliest"
        |  connection.url="jdbc"
        |}
      """.stripMargin

    val r = KafkaReplicationParser.parse(dsl).get
    r.primaryKeys shouldBe Map.empty
  }

  it should "infer the name from topics" in {
    val dsl =
      """
        |replicate {
        |  topics: ["myTopic"]
        |  startingOffsets:"earliest"
        |  connection.url="jdbc"
        |}
      """.stripMargin

    val r = KafkaReplicationParser.parse(dsl).get
    r.name.toString shouldBe UUID.nameUUIDFromBytes("myTopic".getBytes).toString
    r.topics shouldBe Left(List("myTopic"))


    val tdsl =
      """
        |replicate {
        |  topics:  "myTopic1, myTopic2"
        |  startingOffsets:"earliest"
        |  connection.url="jdbc"
        |}
      """.stripMargin

    val tr = KafkaReplicationParser.parse(tdsl).get
    tr.name.toString shouldBe UUID.nameUUIDFromBytes("myTopic1myTopic2".getBytes).toString
    tr.topics shouldBe Left(List("myTopic1", "myTopic2"))

    val sdsl =
      """
        |replicate {
        |  topicPattern: "my.topics.*"
        |  startingOffsets:"earliest"
        |  connection.url="jdbc"
        |}
      """.stripMargin

    val sr = KafkaReplicationParser.parse(sdsl).get
    sr.name.toString shouldBe UUID.nameUUIDFromBytes("my.topics.*".getBytes).toString
    sr.topics shouldBe Right("my.topics.*")
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
      KafkaReplicationParser.parse(dsl).get
    }
  }

  it should "error if both topics and topicPattern are present" in {
    val dsl =
      """
        |{
        |  replicate {
        |  topics:"topic1,topic2"
        |  topicPattern:"exp.identity.*"
        |  startingOffsets:"earliest"
        |  sink: "jdbc"
        |  }
        |}
      """.stripMargin

    intercept[InvalidDslException] {
      KafkaReplicationParser.parse(dsl).get
    }
  }
}
