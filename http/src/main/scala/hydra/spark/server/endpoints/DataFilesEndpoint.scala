package hydra.spark.server.endpoints

import java.io.File

import akka.actor.{ActorRefFactory, ActorSystem}
import akka.http.scaladsl.model.Multipart
import akka.http.scaladsl.model.Multipart.BodyPart
import akka.http.scaladsl.model.StatusCodes.BadRequest
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import akka.util.Timeout
import com.github.vonnagy.service.container.http.routing.RoutedEndpoints
import com.github.vonnagy.service.container.log.LoggingAdapter
import hydra.spark.server.io.FileInfo
import hydra.spark.server.actors.DataFileManager._
import hydra.spark.server.marshalling.{GenericResponse, HydraSparkJsonSupport}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/31/17.
  */
class DataFilesEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraSparkJsonSupport {

  implicit val timeout = Timeout(10.seconds)
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val binariesEndpointResponseFormat = jsonFormat3(DataFilesEndpointResponse)

  lazy val dm = system.actorSelection("/user/service/data_file_manager")

  override val route =
    pathPrefix("data") {
      handleExceptions(excptHandler) {
        getBinaries ~
          path(Segment) { name =>
            post {
              postBinary(name)
            } ~
              delete {
                deleteBinary(name)
              }
          }
      }
    }

  private def getBinaries: Route = {
    get {
      onComplete((dm ? ListDataFiles).mapTo[Seq[FileInfo]]) { response =>
        complete(response)
      }
    }
  }

  private def deleteBinary(name: String): Route = {
    val futResponse = (dm ? DeleteDataFile(name)).map {
      case Deleted => GenericResponse(200, s"File $name deleted.")
      case Error(x) => GenericResponse(400, s"Unable to delete file $name: ${x.getMessage}")
      case r => GenericResponse(500, s"Unexpected response: $r")
    }.recover { case e: Exception => GenericResponse(500, s"Unexpected error: ${e.getMessage}") }

    complete(futResponse)
  }

  private def postBinary(name: String): Route = {
    entity(as[Multipart.FormData]) { formData =>
      val response = formData.parts.mapAsync[(String, Any)](1) {
        case b: BodyPart if b.name == "file" =>
          val file = File.createTempFile("upload", "tmp")
          b.entity.dataBytes.runWith(FileIO.toPath(file.toPath)).map(_ => (b.name -> file))

        case b: BodyPart => b.toStrict(2.seconds).map(strict => (b.name -> strict.entity.data.utf8String))

      }.runFold(Map.empty[String, Any])((map, tuple) => map + tuple)
        .flatMap { data =>
          val future = dm ? StoreDataFile(name, data("file").asInstanceOf[File].getAbsolutePath)
          future.map {
            case Stored(info) => DataFilesEndpointResponse(200, None, Some(info))
            case Error(e) => DataFilesEndpointResponse(400, Some(s"Unable to sve file $name: $e"), None)
            case e => DataFilesEndpointResponse(500, Some(s"Unable to sve file $name: $e"), None)
          }

        }
      complete(response)
    }
  }

  lazy val excptHandler = ExceptionHandler {
    case e: Exception =>
      extractUri { uri =>
        log.warn(s"Request to $uri could not be handled normally")
        complete(BadRequest,
          GenericResponse(400, s"Unable to complete request for ${uri.path.tail} : ${e.getMessage}"))
      }
  }

  case class DataFilesEndpointResponse(status: Int, message: Option[String], dataFileInfo: Option[FileInfo])

}

