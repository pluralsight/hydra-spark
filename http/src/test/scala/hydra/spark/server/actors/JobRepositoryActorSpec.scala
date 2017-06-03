package hydra.spark.server.actors

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import hydra.spark.server.actors.JobRepositoryActor._
import hydra.spark.server.io.{BinaryInfo, JobInfo, JobRepository}
import hydra.spark.server.job.BinaryType
import hydra.spark.server.model.JobStatus.JobStatus
import org.joda.time.DateTime
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}


class JobRepositoryActorSpec extends TestKit(ActorSystem("dao-test")) with ImplicitSender
  with FunSpecLike with Matchers with BeforeAndAfterAll {

  import JobRepositoryActorSpec._

  val daoActor = system.actorOf(JobRepositoryActor.props(DummyDao))

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
  }

  describe("JobDAOActor") {

    it("should respond when saving Binary completes successfully") {
      val now = DateTime.now
      daoActor ! SaveBinary("succeed", BinaryType.Jar, now, Array[Byte]())
      expectMsg(SaveBinaryResult(Success(BinaryInfo("succeed", BinaryType.Jar, now))))
    }

    it("should respond when saving Binary fails") {
      daoActor ! SaveBinary("failOnThis", BinaryType.Jar, DateTime.now, Array[Byte]())
      expectMsgPF(3 seconds) {
        case SaveBinaryResult(Failure(x)) => x.getMessage shouldBe "deliberate failure"
      }
    }

    it("should respond when deleting Binary completes successfully") {
      daoActor ! DeleteBinary("succeed")
      expectMsg(DeleteBinaryResult(true))
    }

    it("should respond when deleting Binary fails") {
      daoActor ! DeleteBinary("failOnThis")
      expectMsgPF(3 seconds) {
        case DeleteBinaryResult(x) => x shouldBe false
      }
    }

    it("should return apps") {
      daoActor ! GetApps(None)
      expectMsg(Apps(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      )))
    }

    it("should get Jobs") {
      daoActor ! GetJobs(1)
      expectMsg(JobInfos(Seq()))
    }

    it("should get binary content") {
      daoActor ! GetBinaryContent("succeed", BinaryType.Jar, DateTime.now)
      expectMsg(BinaryContent(DummyDao.jarContent))
    }
  }
}

object JobRepositoryActorSpec {
  val dt = DateTime.now()
  val dtplus1 = dt.plusHours(1)

  object DummyDao extends JobRepository {

    val jarContent = Array.empty[Byte]

//    override def saveBinary(appName: String, binaryType: BinaryType,
//                            uploadTime: DateTime, binaryBytes: Array[Byte]): Future[BinaryInfo] = {
//      appName match {
//        case "failOnThis" => Future.failed(new Exception("deliberate failure"))
//        case _ => Future.successful(new BinaryInfo(appName, binaryType, uploadTime))
//      }
//    }

    override def getApps: Future[Map[String, (BinaryType, DateTime)]] =
      Future.successful(Map(
        "app1" -> (BinaryType.Jar, dt),
        "app2" -> (BinaryType.Egg, dtplus1)
      ))

//    override def getBinaryContent(appName: String, binaryType: BinaryType,
//                                  uploadTime: DateTime): Array[Byte] = {
//      appName match {
//        case "failOnThis" => throw new Exception("get binary content failure")
//        case _ => jarContent
//      }
//    }
//
//    override def saveJobConfig(jobId: String, jobConfig: Config): Unit = ???

    override def getJobInfo(jobId: String): Future[Option[JobInfo]] = ???

//    override def saveJobInfo(jobInfo: Job): Future[Job] = ???


//    override def getJobConfig(jobId: String): Future[Option[Config]] = ???
//
//    override def deleteBinary(appName: String): Future[Boolean] = {
//      appName match {
//        case "failOnThis" => Future.failed(new Exception("deliberate failure"))
//        case _ => Future.successful(true)
//      }
//    }

    override def getJobs(limit: Int, status: Option[JobStatus]): Future[Seq[JobInfo]] = Future.successful(Seq())

    override def saveJobInfo(jobInfo: JobInfo): Future[JobInfo] = ???
  }

}