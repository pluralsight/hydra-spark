/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.sources.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{ GenericData, GenericRecord }
import org.scalatest.{ FunSpecLike, Matchers }

/**
 * Created by alexsilva on 12/14/16.
 */
class KafkaFormatSpec extends Matchers with FunSpecLike {

  import spray.json._

  import scala.collection.JavaConverters._

  val json =
    """
      |{
      | "test":"json"
      |}
    """.stripMargin

  val keyedJson =
    """
      |{
      | "key":"msg-key",
      | "test":"json"
      |}
    """.stripMargin

  val originalSchema = SchemaBuilder
    .record("HydraSparkTest").namespace("pluralsight")
    .fields()
    .name("test").`type`().stringType().noDefault()
    .endRecord()

  val keyedSchema = SchemaBuilder
    .record("HydraSparkTest").namespace("pluralsight")
    .fields()
    .name("test").`type`().stringType().noDefault()
    .name("key").`type`().stringType().noDefault()
    .endRecord()

  describe("When using string topics") {
    it("Adds the key to string messages that can be parsed as json") {
      val kmmd = KafkaMessageAndMetadata("msg-key", json, "test-topic", 0, 0)
      val keyed = KafkaStringFormat.addKey(kmmd, "key")
      keyed.value.parseJson shouldBe keyedJson.parseJson
      keyed.key shouldBe kmmd.key
    }

    describe("When using json topics") {
      it("Adds the key to json messages") {
        val mapper = new ObjectMapper()
        val original = mapper.reader().readTree(json)
        val kmmd = KafkaMessageAndMetadata("msg-key", original, "test-topic", 0, 0)
        val keyed = KafkaJsonFormat.addKey(kmmd, "key")
        keyed.value shouldBe mapper.reader().readTree(keyedJson)
        keyed.key shouldBe kmmd.key
      }
    }

    describe("When using avro topics") {
      it("Adds the key to generic record messages") {
        val record = new GenericData.Record(originalSchema)
        record.put("test", "json")

        val kmmd = KafkaMessageAndMetadata[String, Object]("msg-key", record, "test-topic", 0, 0)
        val keyed = KafkaAvroFormat.addKey(kmmd, "key")

        //check the schema
        val testSchemaFields = keyedSchema.getFields.asScala
        val msgFields = keyed.value.asInstanceOf[GenericRecord].getSchema.getFields.asScala
        msgFields.map(_.name()) should contain theSameElementsAs testSchemaFields.map(_.name())
        msgFields.map(_.schema()) should contain theSameElementsAs testSchemaFields.map(_.schema())

        val keyedRecord = new GenericData.Record(keyed.value.asInstanceOf[GenericRecord].getSchema)
        keyedRecord.put("test", "json")
        keyedRecord.put("key", "msg-key")

        keyed.value shouldBe keyedRecord
        keyed.key shouldBe kmmd.key
      }
    }
  }
}
