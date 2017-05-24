package hydra.spark.server.marshalling

import java.util.UUID

import akka.http.scaladsl.model.StatusCode

/**
  * Created by alexsilva on 5/1/17.
  */
trait BaseResponse {
  def requestId: String

  def statusCode: StatusCode
}


case class GenericResponse(statusCode: StatusCode, message: String,
                           requestId: String = UUID.randomUUID().toString) extends BaseResponse
