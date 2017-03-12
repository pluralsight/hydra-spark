package hydra.spark.json

/**
  * From  org.apache.spark.sql.catalyst.util.ParseModes (Spark 2)
  *
  * Created by alexsilva on 1/25/17.
  */
object ParseModes {
  val PERMISSIVE_MODE = "PERMISSIVE"
  val DROP_MALFORMED_MODE = "DROPMALFORMED"
  val FAIL_FAST_MODE = "FAILFAST"

  val DEFAULT = PERMISSIVE_MODE

  def isValidMode(mode: String): Boolean = {
    mode.toUpperCase match {
      case PERMISSIVE_MODE | DROP_MALFORMED_MODE | FAIL_FAST_MODE => true
      case _ => false
    }
  }

  def isDropMalformedMode(mode: String): Boolean = mode.toUpperCase == DROP_MALFORMED_MODE
  def isFailFastMode(mode: String): Boolean = mode.toUpperCase == FAIL_FAST_MODE
  def isPermissiveMode(mode: String): Boolean = if (isValidMode(mode))  {
    mode.toUpperCase == PERMISSIVE_MODE
  } else {
    true // We default to permissive is the mode string is not valid
  }
}