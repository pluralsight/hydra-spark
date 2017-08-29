package hydra.spark.api

import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkConf
import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 1/18/17.
  */
class HydraContextSpec extends Matchers with FunSpecLike with SharedSparkContext with ResetSystemProperties {

  describe("When using HydraContext") {
    it("uses the OS current user if none specified") {
      val ctx = HydraContext.builder().sparkContext(sc).getOrCreate()
      ctx.sparkUser shouldBe UserGroupInformation.getCurrentUser().getShortUserName()
    }

    it("uses the DSL user name if specified") {
      val sparkConf = new SparkConf().set("spark.user", "dsl-user")
      val ctx = HydraContext.builder().sparkContext(sc).getOrCreate()
      ctx.getCurrentUserName(sparkConf) shouldBe "dsl-user"
    }

    it("uses SPARK_USER as the user if specified") {
      setEnv("SPARK_USER", "test-spark")
      val ctx = HydraContext.builder().sparkContext(sc).getOrCreate()
      ctx.sparkUser shouldBe "test-spark"
      removeEnv("SPARK_USER")
    }

    it("manipulates properties") {
      val ctx = HydraContext.builder().sparkContext(sc).getOrCreate()
      ctx.setProperty("test", "value")
      ctx.getProperty("test") shouldBe "value"
      ctx.setProperty("test", null)
      ctx.getProperties.getProperty("test", "default") shouldBe "default"
    }

    it("has accumulators") {
      val ctx = HydraContext.builder().sparkContext(sc).getOrCreate()
      val a = ctx.getAccumulator("test")
      a.add(1)
      a.value shouldBe 1
      ctx.getAccumulator("test").add(1)
      ctx.getAccumulator("test").value shouldBe 2
    }

    it("Initializes listeners") {

    }
  }

  private def setEnv(key: String, value: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.put(key, value)
  }

  private def removeEnv(key: String) = {
    val field = System.getenv().getClass.getDeclaredField("m")
    field.setAccessible(true)
    val map = field.get(System.getenv()).asInstanceOf[java.util.Map[java.lang.String, java.lang.String]]
    map.remove(key)
  }
}
