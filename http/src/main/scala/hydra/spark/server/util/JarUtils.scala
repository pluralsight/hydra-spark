package hydra.spark.server.util

/**
  * Created by alexsilva on 5/31/17.
  */
object JarUtils {
  def binaryIsZip(binaryBytes: Array[Byte]): Boolean = {
    binaryBytes.length > 4 &&
      // For now just check the first few bytes are the ZIP signature: 0x04034b50 little endian
      binaryBytes(0) == 0x50 && binaryBytes(1) == 0x4b && binaryBytes(2) == 0x03 && binaryBytes(3) == 0x04
  }
}
