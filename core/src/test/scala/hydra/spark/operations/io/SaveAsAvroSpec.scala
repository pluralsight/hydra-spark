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

package hydra.spark.operations.io

import com.google.common.io.Files
import hydra.spark.api.{Invalid, Source, Valid}
import hydra.spark.testutils.{ListOperation, SharedSparkContext}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{RegexFileFilter, TrueFileFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by alexsilva on 6/22/16.
  */
class SaveAsAvroSpec extends Matchers with FunSpecLike with Inside with BeforeAndAfterAll
  with SharedSparkContext {
  val tmpDir = Files.createTempDir()

  override def afterAll(): Unit = {
    super.afterAll()
    tmpDir.delete()
  }

  describe("When Saving as Avro") {
    it("is invalid when resource schema can't be found") {
      inside(SaveAsAvro(tmpDir.getAbsolutePath, "classpath:unknown.avsc", None, Map.empty).validate) {
        case Invalid(errors) =>
          errors.seq.head.message.indexOf("FileNotFoundException") shouldNot be(-1)
          errors.size shouldBe 1
      }
    }
    it("is invalid when a registry schema can't be found") {
      inside(SaveAsAvro(tmpDir.getAbsolutePath, "registry-schema", None, Map.empty).validate) {
        case Invalid(errors) =>
          errors.size shouldBe 1
      }
    }

    it("Should save a string source") {
      val t = SaveAsAvro(tmpDir.getAbsolutePath, "classpath:schema.avsc", None, Map.empty,
        overwrite = true)
      t.transform(AvroSpecSource.createDF(ss))
      val output = FileUtils.listFiles(tmpDir, new RegexFileFilter("^.*.avro$"), TrueFileFilter.INSTANCE)
      val records = output.asScala.map { file =>
        val reader = DataFileReader.openReader(file, new GenericDatumReader[GenericRecord]())
        reader.iterator().asScala.toSeq
      }

      val avroRecords = records.flatten
      avroRecords should have size AvroSpecSource.avroMsgs.size
      avroRecords.map(_.toString) should contain theSameElementsAs AvroSpecSource.avroMsgs.map(_.toString)
    }
  }

  override def beforeEach() = ListOperation.reset
}

object AvroSpecSource extends Source[String] {

  import hydra.spark.util.RDDConversions._

  val msgs = for (i <- 0 to 10)
    yield
      s"""{"messageId": $i, "messageValue": "value-${i}"}""".stripMargin

  val schema = new Schema.Parser().parse(Thread.currentThread()
    .getContextClassLoader.getResourceAsStream("schema.avsc"))

  val avroMsgs = msgs.map { msg =>
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().jsonDecoder(schema, msg)
    val record = reader.read(null, decoder)
    record
  }

  override def createStream(sc: StreamingContext): DStream[String] = {
    val rdd = sc.sparkContext.parallelize(msgs, 1)
    val lines = mutable.Queue[RDD[String]](rdd)
    sc.queueStream[String](lines, false, rdd)
  }

  override def validate = Valid

  override def createDF(ctx: SparkSession): DataFrame = {
    import ctx.implicits._
    ctx.read.json(ctx.createDataset(msgs))
  }

  override def toDF(rdd: RDD[String]): DataFrame = rdd.toDF
}
