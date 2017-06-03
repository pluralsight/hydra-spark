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
import hydra.spark.server.actors.BinaryFileManager.{DeleteBinary, ListBinaries, StoreBinary}
import hydra.spark.server.actors.Messages._
import hydra.spark.server.io.FileInfo
import hydra.spark.server.job.BinaryType
import hydra.spark.server.marshalling.{GenericResponse, HydraSparkJsonSupport}

import scala.concurrent.duration._

/**
  * Created by alexsilva on 5/31/17.
  */
class JarsEndpoint(implicit val system: ActorSystem, implicit val actorRefFactory: ActorRefFactory)
  extends RoutedEndpoints with LoggingAdapter with HydraSparkJsonSupport {

  implicit val timeout = Timeout(10.seconds)
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  lazy val bm = system.actorSelection("/user/service/binary_file_manager")

  override val route =
    pathPrefix("jars") {
      handleExceptions(excptHandler) {
        getJars ~
          path(Segment) { name =>
            post {
              addJar(name)
            } ~
              delete {
                deleteJar(name)
              }
          }
      }
    }


  private def getJars: Route = {
    get {
      complete((bm ? ListBinaries(Some(BinaryType.Jar))).mapTo[Seq[FileInfo]])
    }
  }

  private def deleteJar(name: String): Route = {
    val futResponse = (bm ? DeleteBinary(name)).map {
      case Deleted => GenericResponse(200, s"Jar $name deleted.")
      case Error(x) => GenericResponse(400, s"Unable to delete jar $name: ${x.getMessage}")
      case r => GenericResponse(500, s"Unexpected response: $r")
    }.recover { case e: Exception => GenericResponse(500, s"Unexpected error: ${e.getMessage}") }

    complete(futResponse)
  }

  private def addJar(name: String): Route = {
    entity(as[Multipart.FormData]) { formData =>
      val response = formData.parts.mapAsync[(String, Any)](1) {
        case b: BodyPart if b.name == "file" =>
          val file = File.createTempFile("upload", "tmp")
          b.entity.dataBytes.runWith(FileIO.toPath(file.toPath)).map(_ => (b.name -> file))

        case b: BodyPart => b.toStrict(2.seconds).map(strict => (b.name -> strict.entity.data.utf8String))

      }.runFold(Map.empty[String, Any])((map, tuple) => map + tuple)
        .flatMap { data =>
          val future = bm ? StoreBinary(name, BinaryType.Jar, data("file").asInstanceOf[File].getAbsolutePath)
          future.map {
            case Stored(info) => FileEndpointResponse(200, None, Some(info))
            case Error(e) => FileEndpointResponse(400, Some(s"Unable to sve file $name: $e"), None)
            case e => FileEndpointResponse(500, Some(s"Unable to sve file $name: $e"), None)
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

}

