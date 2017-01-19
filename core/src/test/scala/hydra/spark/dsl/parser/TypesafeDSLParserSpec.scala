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

package hydra.spark.dsl.parser

import java.io.File

import com.typesafe.config.ConfigFactory
import hydra.spark.api.SparkContextFactory
import hydra.spark.dispatch.context.{ DefaultSparkContextFactory, StreamingContextFactory }
import org.scalatest.{ FunSpecLike, Matchers }

/**
 * Created by alexsilva on 1/9/17.
 */
class TypesafeDSLParserSpec extends Matchers with FunSpecLike {

  val customFactoryDsl =
    """
      |      {
      |      	"dispatch": {
      |      		"version": 1,
      |      		"name": "test-defaults-job",
      |         "spark.master":"local[*]",
      |         "context-factory":"hydra.spark.dsl.parser.TestSparkContextFactory",
      |      	"source": {
      |      		"kafka": {
      |      			"topics": {
      |      				"test.topic": {
      |      					"format": "json",
      |      					"start": -1
      |      				}
      |      			}
      |      		}
      |      	},
      |      	"operations": {
      |      		"1:value-filter": {
      |      			"column": "ip",
      |      			"value": "alex"
      |      		}
      |      	}
      |      }
      |      }
    """.
      stripMargin

  val simpleDsl = Thread.currentThread().getContextClassLoader.getResource("simple-dsl.json").getFile
  val streamingDsl = Thread.currentThread().getContextClassLoader.getResource("simple-streaming-dsl.json").getFile
  describe("When reading a DSL") {
    it("uses the default factory") {
      val d = TypesafeDSLParser()(ConfigFactory.parseFile(new File(simpleDsl)))
      d.fact shouldBe a[SparkContextFactory]
    }
    it("uses the streaming factory") {
      val sd = TypesafeDSLParser()(ConfigFactory.parseFile(new File(streamingDsl)))
      sd.fact shouldBe a[StreamingContextFactory]
    }
    it("uses a custom factory") {
      val cd = TypesafeDSLParser().parse(customFactoryDsl)
      cd.fact shouldBe a[TestSparkContextFactory]
    }
  }
}

private[this] class TestSparkContextFactory extends DefaultSparkContextFactory