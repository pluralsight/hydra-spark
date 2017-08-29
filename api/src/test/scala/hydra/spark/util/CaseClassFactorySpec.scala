package hydra.spark.util

import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/3/17.
  */
class CaseClassFactorySpec extends Matchers with FunSpecLike {
  describe("When using ReflectionUtils") {
    it("Instantiates a case class with constructor params") {
      new CaseClassFactory(classOf[TestCase]).buildWith(Seq("name", 120,2.seconds)) shouldBe
        TestCase("name", 120, 2 seconds)
    }

    it("Rejects non-case classes") {
      intercept[IllegalArgumentException] {
        new CaseClassFactory(classOf[Tester]).buildWith(Seq("name", 120))
      }
    }
  }
}

class Tester

case class TestCase(name: String, value: Int, duration: Duration = 1.second)






