package hydra.spark.json

/**
  * From org.apache.spark.sql.catalyst.json.JsonOptions (Spark 2)
  *
  * Created by alexsilva on 1/25/17.
  */

import java.util.Locale

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import hydra.spark.internal.Logging
import hydra.spark.util.CompressionCodecs
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.execution.datasources.CaseInsensitiveMap

/**
  * Options for parsing JSON data into Spark SQL rows.
  *
  * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
  */
 class JSONOptions(
                                @transient private val parameters: CaseInsensitiveMap)
  extends Logging with Serializable {

  def this(parameters: Map[String, String]) = this(new CaseInsensitiveMap(parameters))

  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  val primitivesAsString =
    parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false)
  val prefersDecimal =
    parameters.get("prefersDecimal").map(_.toBoolean).getOrElse(false)
  val allowComments =
    parameters.get("allowComments").map(_.toBoolean).getOrElse(false)
  val allowUnquotedFieldNames =
    parameters.get("allowUnquotedFieldNames").map(_.toBoolean).getOrElse(false)
  val allowSingleQuotes =
    parameters.get("allowSingleQuotes").map(_.toBoolean).getOrElse(true)
  val allowNumericLeadingZeros =
    parameters.get("allowNumericLeadingZeros").map(_.toBoolean).getOrElse(false)
  val allowNonNumericNumbers =
    parameters.get("allowNonNumericNumbers").map(_.toBoolean).getOrElse(true)
  val allowBackslashEscapingAnyCharacter =
    parameters.get("allowBackslashEscapingAnyCharacter").map(_.toBoolean).getOrElse(false)
  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  private val parseMode = parameters.getOrElse("mode", "PERMISSIVE")
  val columnNameOfCorruptRecord = parameters.get("columnNameOfCorruptRecord")

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ"), Locale.US)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    log.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  val failFast = ParseModes.isFailFastMode(parseMode)
  val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  val permissive = ParseModes.isPermissiveMode(parseMode)

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
  }
}
