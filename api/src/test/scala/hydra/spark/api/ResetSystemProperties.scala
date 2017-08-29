package hydra.spark.api

import java.util.Properties

import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{BeforeAndAfterEach, Suite}

private[hydra] trait ResetSystemProperties extends BeforeAndAfterEach {
  this: Suite =>
  var oldProperties: Properties = null

  override def beforeEach(): Unit = {
    oldProperties = SerializationUtils.clone(System.getProperties)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      System.setProperties(oldProperties)
      oldProperties = null
    }
  }
}