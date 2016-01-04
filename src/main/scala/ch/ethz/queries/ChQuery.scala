package ch.ethz.queries

import ch.ethz.tell.TellContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import java.util.Calendar

object ChQuery extends Logging {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.sql.tell.chunkSizeBig", (2L * 1024L * 1024L * 1024L).toString)

    val sc = new SparkContext(conf)
    val context = new TellContext(sc)

    val i: Int = 1
    val query = Class.forName(f"ch.ethz.queries.chb.Q${i}%d").newInstance.asInstanceOf[ChQuery]

    logInfo(s"Running query ${i}")
    val start = System.nanoTime()
    context.startTransaction()

    val data = query.executeQuery(context)
    data.show(100)

    context.commitTransaction()
    val end = System.nanoTime()
    logInfo(s"Running query ${i} took ${(end - start) / 1000000}ms")
  }
}

class ChQuery {

  // have the reference date as it appears in many places
  val calendar = Calendar.getInstance()

  val referenceDate1999: Long = {
    calendar.set(1999, 1, 1)
    calendar.getTimeInMillis * 1000L * 1000L  // create nano seconds
  }

  val referenceDate2007: Long = {
    calendar.set(2007, 1, 2)
    calendar.getTimeInMillis * 1000L * 1000L  // create nano seconds
  }

  val referenceDate2010: Long = {
    calendar.set(2010, 5, 23, 12, 0)
    calendar.getTimeInMillis * 1000L * 1000L  // create nano seconds
  }

  val referenceDate2012: Long = {
    calendar.set(2012, 1, 2)
    calendar.getTimeInMillis * 1000L * 1000L  // create nano seconds
  }

  val referenceDate2020First: Long = {
    calendar.set(2020, 1, 1)
    calendar.getTimeInMillis * 1000L * 1000L  // create nano seconds
  }

  val referenceDate2020Second: Long = {
    calendar.set(2020, 1, 2)
    calendar.getTimeInMillis * 1000L * 1000L  // create nano seconds
  }

  /**
   * implemented in children classes and hold the actual query
   */
  def executeQuery(context: TellContext): DataFrame = ???
}
