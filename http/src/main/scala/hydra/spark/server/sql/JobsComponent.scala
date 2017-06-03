package hydra.spark.server.sql

import hydra.spark.server.io.{BinaryInfo, JobInfo}
import hydra.spark.server.job.BinaryType
import hydra.spark.server.model.JobStatus._
import hydra.spark.server.model.{Job, JobStatus}
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by alexsilva on 6/1/17.
  */

trait JobsComponent extends BinariesComponent {

  this: DatabaseComponent with ProfileComponent =>

  import profile.api._
  import slick.lifted.Tag

  implicit val jobStatusMapper = MappedColumnType.base[JobStatus, String](e => e.toString, s => JobStatus.withName(s))

  lazy val binaries = TableQuery[BinariesTable]

  class JobsTable(tag: Tag) extends Table[Job](tag, "JOBS") {
    def jobId = column[String]("JOB_ID", O.PrimaryKey)

    def contextName = column[String]("CONTEXT_NAME")

    def binId = column[Int]("BIN_ID")

    def classPath = column[String]("CLASSPATH")

    def status = column[JobStatus]("STATUS")

    def startTime = column[DateTime]("START_TIME")

    def endTime = column[Option[DateTime]]("END_TIME")

    def error = column[Option[String]]("ERROR")

    def * = (jobId.?, contextName, binId, classPath, status, startTime, endTime, error) <> (Job.tupled, Job.unapply)

    def binary = foreignKey("BINARY", binId, binaries)(_.id)
  }

  object JobsRepository extends Repository[JobsTable, String](profile, db) {

    val table = TableQuery[JobsTable]

    val binaryTable = TableQuery[BinariesTable]

    val createQuery = table returning table.map(_.jobId) into ((item, id) => item.copy(jobId = Some(id)))

    def create(t: Job): Future[Job] = {
      val action = createQuery += t
      db.run(action)
    }

    def getjobInfo(jobId: String) = {
      db.run(table.filter(_.jobId === jobId).result.headOption)
    }

    def getId(table: JobsTable) = table.jobId

    def getJobs(limit: Int, statusOpt: Option[JobStatus] = None)(implicit ec: ExecutionContext): Future[Seq[JobInfo]] = {

      val joinQuery = for {
        bin <- binaries
        j <- table if j.binId === bin.id && (statusOpt match {
        // !endTime.isDefined
        case Some(JobStatus.Running) => !j.endTime.isDefined && !j.error.isDefined
        // endTime.isDefined && error.isDefined
        case Some(JobStatus.Error) => j.error.isDefined
        // not RUNNING AND NOT ERROR
        case Some(JobStatus.Finished) => j.endTime.isDefined && !j.error.isDefined
        case _ => true
      })
      } yield {
        (j.jobId,
          j.contextName,
          bin.appName,
          bin.binaryType,
          bin.uploadTime,
          j.classPath,
          j.startTime,
          j.endTime,
          j.status,
          j.error)
      }
      val sortQuery = joinQuery.sortBy(_._7.desc)
      val limitQuery = sortQuery.take(limit)
      // Transform the each row of the table into a map of JobInfo values
      for (r <- db.run(limitQuery.result)) yield {
        r.map { case (id, context, app, binType, upload, classpath, start, end, status, err) =>
          JobInfo(id,
            context,
            BinaryInfo(app, BinaryType.fromString(binType), upload),
            classpath,
            start,
            end,
            status,
            err.map(new Throwable(_)))
        }
      }
    }


    def getJobInfo(jobId: String)(implicit ec: ExecutionContext): Future[Option[JobInfo]] = {

      val innerJoin = for {
        (j, b) <- table join BinariesRepository.table on { case (j, b) => j.binId === b.id && j.jobId === jobId }
      } yield {
        (j.jobId,
          j.contextName,
          b.appName,
          b.binaryType,
          b.uploadTime,
          j.classPath,
          j.startTime,
          j.endTime,
          j.status,
          j.error)
      }
      for (r <- db.run(innerJoin.result)) yield {
        r.map {
          case (id, context, app, binType, upload, classpath, start, end, status, err) =>
            JobInfo(id,
              context,
              BinaryInfo(app, BinaryType.fromString(binType), upload),
              classpath,
              start,
              end,
              status,
              err.map(new Throwable(_)))

        }
      }.headOption
    }
  }

}