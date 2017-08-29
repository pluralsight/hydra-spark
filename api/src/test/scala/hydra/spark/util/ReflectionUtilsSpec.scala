package hydra.spark.util

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/3/17.
  */
class ReflectionUtilsSpec extends Matchers with FunSpecLike {
  describe("When using ReflectionUtils") {
    it("Instantiates a class with constructor params") {
      ReflectionUtils.instantiateType[TestClass](List("value")) shouldBe TestClass("value")
      ReflectionUtils.instantiateClass(classOf[TestClass], List("value")) shouldBe TestClass("value")
    }

    it("Identifies companion objects") {
      ReflectionUtils.companionOf[TestClass] shouldBe TestClass
      ReflectionUtils.companionOf(classOf[TestClass]) shouldBe TestClass
    }

    it("Formats class names without dollar signs") {
      val obj = ReflectionUtils.getFormattedClassName(TestClass)
      obj shouldBe "TestClass"

      val obj1 = ReflectionUtils.getFormattedClassName(new String("RAR"))
      obj1 shouldBe "String"
    }

  }

}

case class TestClass(value: String)

object TestClass {
  def apply(n: Int) = new TestClass(n.toString)
}


