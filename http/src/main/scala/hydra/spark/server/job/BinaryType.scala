package hydra.spark.server.job

import akka.http.scaladsl.model.MediaType.NotCompressible
import akka.http.scaladsl.model.{ContentType, MediaType, MediaTypes}

/**
  * Created by alexsilva on 5/26/17.
  */
trait BinaryType {
  def extension: String

  def name: String

  def mediaType: MediaType.Binary

  val contentType = ContentType(mediaType)
}

object BinaryType {

  case object Jar extends BinaryType {
    val extension = "jar"
    val name = "Jar"
    val mediaType = MediaTypes.`application/java-archive`
  }

  case object Data extends BinaryType {
    val extension = "dat"
    val name = "Data"
    val mediaType = MediaType.customBinary("application", "data", NotCompressible, List("dat"))
  }

  case object Egg extends BinaryType {
    val extension = "egg"
    val name = "Egg"
    val mediaType = MediaType.customBinary("application", "python-archive", NotCompressible, List("egg"))
  }

  def fromString(typeString: String): BinaryType = typeString match {
    case "Jar" => Jar
    case "Egg" => Egg
    case "Data" => Data
    case _ => throw new IllegalArgumentException(s"$typeString is not a valid binary type.")
  }

  def fromMediaType(mediaType: MediaType): Option[BinaryType] = mediaType match {
    case m if m == Jar.mediaType => Some(Jar)
    case m if m == Egg.mediaType => Some(Egg)
    case m if m == Data.mediaType => Some(Data)
    case _ => None
  }
}