package hydra.spark.server.marshalling

/**
  * Created by alexsilva on 5/1/17.
  */
trait BaseResponse

case class GenericResponse(status: Int, message: String) extends BaseResponse

case class JobSubmittedResponse(streamId: String, logFile: String)

case class JobStatusResponse(requestId: String, sparkAppId: Option[String], state: String) extends BaseResponse