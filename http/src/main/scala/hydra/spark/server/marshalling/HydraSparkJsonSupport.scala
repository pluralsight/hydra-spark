package hydra.spark.server.marshalling

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import hydra.spark.server.io.FileInfo
import hydra.spark.server.job.BinaryType
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import spray.json.{DefaultJsonProtocol, JsString, JsValue, RootJsonFormat}

import scala.util.Try

/**
  * Created by alexsilva on 4/28/17.
  */
trait HydraSparkJsonSupport extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object DateTimeFormat extends RootJsonFormat[DateTime] {

    val formatter = ISODateTimeFormat.basicDateTimeNoMillis

    def write(obj: DateTime): JsValue = {
      JsString(formatter.print(obj))
    }

    def read(json: JsValue): DateTime = json match {
      case JsString(s) => try {
        formatter.parseDateTime(s)
      }
      catch {
        case t: Throwable => error(s)
      }
      case _ =>
        error(json.toString())
    }

    def error(v: Any): DateTime = {
      val example = formatter.print(0)
      spray.json.deserializationError(f"'$v' is not a valid date value. Dates must be in compact ISO-8601 format" +
        f", e.g. '$example'")
    }
  }

  implicit object BinaryTypeFormat extends RootJsonFormat[BinaryType] {

    def write(obj: BinaryType): JsValue = {
      JsString(obj.name)
    }

    def read(json: JsValue): BinaryType = json match {
      case JsString(s) => Try(BinaryType.fromString(s))
        .recover { case t: Throwable => spray.json.deserializationError(f"'${t.getMessage}") }.get
      case _ => spray.json.deserializationError("Not a valid binary type.")
    }

  }


  implicit val jobStatusResponseFormat = jsonFormat3(JobStatusResponse)
  implicit val dataFileInfoFormat = jsonFormat4(FileInfo)
  implicit val genericResponseFormat = jsonFormat2(GenericResponse)
  implicit val jobSubmittedResponseFormat = jsonFormat2(JobSubmittedResponse)

}
