package hydra.spark.server.util

import org.joda.time.{DateTime, DateTimeComparator}

object DateUtils {

  private val dateComparator = DateTimeComparator.getInstance()

  implicit def dateTimeToScalaWrapper(dt: DateTime): DateTimeWrapper = new DateTimeWrapper(dt)

  class DateTimeWrapper(dt: DateTime) extends Ordered[DateTime] with Ordering[DateTime] {
    def compare(that: DateTime): Int = dateComparator.compare(dt, that)

    def compare(a: DateTime, b: DateTime): Int = dateComparator.compare(a, b)
  }

}