package hydra.spark.server.io

import com.typesafe.config.ConfigFactory
import hydra.spark.server.TestJars
import hydra.spark.server.dao.DAOSpecBase
import hydra.spark.server.job.BinaryType
import hydra.spark.server.model.JobStatus
import hydra.spark.server.sql.{FlywaySupport, H2Persistence}
import org.joda.time.DateTime
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global


class JobRepositorySpec extends Matchers with FunSpecLike with BeforeAndAfterAll
  with H2Persistence with DAOSpecBase with ScalaFutures with TestJars {

  var dao = new SlickJobRepository

  val expectedJobInfo = jobInfoNoEndNoErr
  val jobInfoSomeEndNoErr: JobInfo = genJobInfo(jarInfo, true, false, JobStatus.Unknown, false)
  val jobInfoNoEndSomeErr: JobInfo = genJobInfo(jarInfo, false, true, JobStatus.Error, false)
  val jobInfoSomeEndSomeErr: JobInfo = genJobInfo(jarInfo, true, true, JobStatus.Error, false)

  override def beforeAll(): Unit = {
    import scala.concurrent.duration._
    super.beforeAll()
    FlywaySupport.migrate(ConfigFactory.load().getConfig("h2-db"))
    Await.result(new SlickBinaryRepository("/tmp/hydra-spark/test")
      .saveBinary(jarInfo.appName, BinaryType.Jar, jarInfo.uploadTime, jarBytes), 10 seconds)
  }

  describe("Basic saveJobInfo() and getJobs() tests") {
    it("should provide an empty Seq on getJobs() for an empty JOBS table") {
      whenReady(dao.getJobs(1)) { x => x shouldBe Seq.empty[JobInfo] }
    }

    it("should save a new JobInfo and get the same JobInfo") {
      whenReady(dao.saveJobInfo(jobInfoNoEndNoErr)) { d =>
        // get some JobInfos
        val jobs = dao.getJobs(10)
        whenReady(jobs) { j =>
          j.head.jobId should equal(jobId)
          j.head should equal(expectedJobInfo)
        }
      }
    }

    it("should be able to get previously saved JobInfo") {
      whenReady(dao.getJobInfo(jobId)) { i => i should equal(Some(expectedJobInfo)) }
    }

    it("Save another new jobInfo, bring down DB, bring up DB, should JobInfos from DB") {
      val jobInfo2 = genJobInfo(jarInfo, false, false, JobStatus.Unknown, true)
      val jobId2 = jobInfo2.jobId
      val expectedJobInfo2 = jobInfo2
      // jobInfo previously saved

      // save new job config
      dao.saveJobInfo(jobInfo2)
      dao = new SlickJobRepository

      val jobs = dao.getJobs(2)
      whenReady(jobs) { j =>
        val jobIds = j.map(_.jobId)
        jobIds should equal(Seq(jobId2, jobId))
        j should equal(Seq(expectedJobInfo2, expectedJobInfo))
      }
    }

    it("saving a JobInfo with the same jobId should update the JOBS table") {
      import scala.concurrent.duration._
      val expectedSomeEndNoErr = jobInfoSomeEndNoErr
      val expectedSomeEndSomeErr = jobInfoSomeEndSomeErr

      val info = genJarInfo(true, false)
      info.uploadTime should equal(jarInfo.uploadTime)

      val timeout = 10.seconds

      // Get all jobInfos
      val jobs: Seq[JobInfo] = Await.result(dao.getJobs(2), timeout)

      // First Test
      jobs.size should equal(2)
      jobs.last should equal(expectedJobInfo)

      val jobInfoNoEndSomeErrEarlier = jobInfoNoEndSomeErr.copy(startTime = DateTime.now.minusHours(1))
      Await.result(dao.saveJobInfo(jobInfoNoEndSomeErrEarlier), timeout)
      val jobs2 = Await.result(dao.getJobs(2), timeout)

      jobs2.size should equal(2)
      jobs2.last.endTime should equal(None)
      jobs2.last.error.isDefined should equal(true)
      intercept[Throwable] {
        jobs2.last.error.map(throw _)
      }
      jobs2.last.error.get.getMessage should equal(throwable.getMessage)

      // Third Test
      val njob = jobInfoSomeEndNoErr.copy(startTime = DateTime.now.minusHours(1))
      Await.result(dao.saveJobInfo(njob), timeout)
      val jobs3 = Await.result(dao.getJobs(2), timeout)
      jobs3.size should equal(2)
      jobs3.last.error.isDefined should equal(false)
      jobs3.last should equal(njob)

      // Fourth Test
      // Cannot compare JobInfos directly if error is a Some(Throwable) because
      // Throwable uses referential equality
      Await.result(dao.saveJobInfo(jobInfoSomeEndSomeErr.copy(startTime = DateTime.now.minusHours(1))),timeout)
      val jobs4 = Await.result(dao.getJobs(2), timeout)
      jobs4 foreach println
      jobs4.size should equal(2)
      jobs4.last.endTime should equal(expectedSomeEndSomeErr.endTime)
      jobs4.last.error.isDefined should equal(true)
      intercept[Throwable] {
        jobs4.last.error.map(throw _)
      }
      jobs4.last.error.get.getMessage should equal(throwable.getMessage)
    }
    it("retrieve by status equals running should be no end and no error") {
      //save some job insure exist one running job
      val dt1 = DateTime.now()
      val dt2 = Some(DateTime.now())
      val someError = Some(new Throwable("test-error"))
      val finishedJob: JobInfo = JobInfo("test-finished", "test", jarInfo, "test-class", dt1, dt2, JobStatus.Finished, None)
      val errorJob: JobInfo = JobInfo("test-error", "test", jarInfo, "test-class", dt1, dt2, JobStatus.Error, someError)
      val runningJob: JobInfo = JobInfo("test-running", "test", jarInfo, "test-class", dt1, None, JobStatus.Running, None)
      whenReady(dao.saveJobInfo(finishedJob)
        .flatMap(_ => dao.saveJobInfo(runningJob))
        .flatMap(_ => dao.saveJobInfo(errorJob))) { _ =>
        //retrieve by status equals RUNNING
        whenReady(dao.getJobs(1, Some(JobStatus.Running))) { j =>
          j.head.endTime.isDefined should equal(false)
          j.head.error.isDefined should equal(false)
        }
      }
    }
    it("retrieve by status equals finished should be some end and no error") {
      whenReady(dao.getJobs(1, Some(JobStatus.Finished))) { j =>
        j.head.endTime.isDefined should equal(true)
        j.head.error.isDefined should equal(false)
      }
    }

    it("retrieve by status equals error should be some error") {
      whenReady(dao.getJobs(1, Some(JobStatus.Error))) { j =>
        j.head.error.isDefined should equal(true)
      }
    }
  }
}

 