package ch.ethz.queries.micro

import ch.ethz.queries.ChQuery
import ch.ethz.queries.chb.ChTSchema
import ch.ethz.tell._

/**
 * Micro QueryA
 */
class Q1 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val oSchema = ChTSchema.orderLineSch

    // prepare date selection
    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.GREATER, oSchema.getField("ol_delivery_d").index, referenceDate2007)
    val orderLineQuery = new ScanQuery
    orderLineQuery.addSelection(dateSelection)

    val orderline = orderLineRdd(scc, orderLineQuery, ChTSchema.orderLineSch).toDF()
    logger.info("[MicroQuery %d] Filter at storage level: %s".format(1, orderline.printSchema))
    timeCollect(orderline, 1)

    val orderline2 = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF()
    orderline2.filter($"ol_delivery_d" >= 20071212)
    logger.info("[MicroQuery %s] Filter at compute level: %s".format(1, orderline2.printSchema))
    timeCollect(orderline2, 1)

    scc.sparkContext.stop()
  }
}
