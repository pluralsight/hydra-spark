package hydra.spark.api

import org.scalatest.{BeforeAndAfterEach, FunSpecLike, Matchers}

class ValidationSpec extends Matchers with FunSpecLike {

  describe("When using ValidationResult") {
    it("successfully evaluates two valid results as valid") {
      val v1 = Valid
      val v2 = () => Valid
      v1.and(v2) shouldBe Valid
    }
    it("successfully evaluates three valid results as valid") {
      val v1 = Valid
      val v2 = () => Valid
      val v3 = () => Valid
      v1.and(v2).and(v3) shouldBe Valid
    }
    it("successfully evaluates any invalid results as invalid") {
      val v1 = Valid
      val v2 = () => Valid
      val inv = Invalid("validation-test", "test message")
      val inv2 = () => Invalid("validation-test", "test message2")
      val inv3 = () => Invalid("validation-test", "test message3")
      // try several combinations, first invalid encountered should be what is returned
      inv.and(inv2) shouldBe Invalid("validation-test", "test message")
      inv.and(inv2).and(inv3) shouldBe Invalid("validation-test", "test message")
      v1.and(v2).and(inv3) shouldBe Invalid("validation-test", "test message3")
    }
  }
}
