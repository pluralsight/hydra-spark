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

package hydra.spark.test.utils

/**
  * Created by alexsilva on 1/9/17.
  */

import hydra.spark.api.{ContextLike, DispatchDetails}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: Suite =>

  @transient private var _sc: SparkContext = _

  @transient private var _scl: ContextLike = _

  def sc: SparkContext = _sc

  def scl:ContextLike = _scl

  var conf = new SparkConf(false)
    .setMaster("local[*]")
    .setAppName("hydra-test")
    .set("spark.ui.enabled", "false")
    .set("spark.driver.allowMultipleContexts", "false")
    .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)

  override def beforeAll() {
    super.beforeAll()
    _sc = new SparkContext(conf)
    _scl = new ContextLike {
      override def stop() = _sc.stop()

      override def isValidDispatch(d: DispatchDetails[_]) = !d.isStreaming

      override def sparkContext = _sc
    }
  }

  override def afterAll() {
    try {
      LocalSparkContext.stop(_sc)
      _sc = null
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    DebugFilesystem.assertNoOpenStreams()
  }
}
