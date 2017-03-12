package hydra.spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress._

/**
  * Created by alexsilva on 1/25/17.
  */
object CompressionCodecs {
  private val shortCompressionCodecNames = Map(
    "none" -> null,
    "uncompressed" -> null,
    "bzip2" -> classOf[BZip2Codec].getName,
    "deflate" -> classOf[DeflateCodec].getName,
    "gzip" -> classOf[GzipCodec].getName,
    "lz4" -> classOf[Lz4Codec].getName,
    "snappy" -> classOf[SnappyCodec].getName)

  /**
    * Return the full version of the given codec class.
    * If it is already a class name, just return it.
    */
  def getCodecClassName(name: String): String = {
    val codecName = shortCompressionCodecNames.getOrElse(name.toLowerCase, name)
    try {
      // Validate the codec name
      if (codecName != null) {
        Class.forName(codecName, true, getContextOrSparkClassLoader)
      }
      codecName
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Codec [$codecName] " +
          s"is not available. Known codecs are ${shortCompressionCodecNames.keys.mkString(", ")}.")
    }
  }

  private def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  private def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
    * Set compression configurations to Hadoop `Configuration`.
    * `codec` should be a full class path
    */
  def setCodecConfiguration(conf: Configuration, codec: String): Unit = {
    if (codec != null) {
      conf.set("mapreduce.output.fileoutputformat.compress", "true")
      conf.set("mapreduce.output.fileoutputformat.compress.type", CompressionType.BLOCK.toString)
      conf.set("mapreduce.output.fileoutputformat.compress.codec", codec)
      conf.set("mapreduce.map.output.compress", "true")
      conf.set("mapreduce.map.output.compress.codec", codec)
    } else {
      // This infers the option `compression` is set to `uncompressed` or `none`.
      conf.set("mapreduce.output.fileoutputformat.compress", "false")
      conf.set("mapreduce.map.output.compress", "false")
    }
  }
}
