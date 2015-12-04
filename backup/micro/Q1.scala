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
  override def execute(st: String, cm: String, partNum: Int, cs: Long, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, partNum, cs)

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

//    val orderline = orderLineRdd(scc, orderLineQuery, ChTSchema.orderLineSch).toDF()
//    logger.info("[MICRO_QUERY %d] Filter at storage level: %s".format(1, orderline.printSchema))
//    timeCollect(orderline, 1)

    val orderline2 = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF()
//    orderline2.filter($"ol_delivery_d" >= 20071212)

    logger.info("[MICRO_QUERY %s] Filter at compute level: %s".format(1, orderline2.printSchema))
    logger.info("[MICRO_QUERY %s] Physical plan: %s".format(1, orderline2.explain(true)))
    timeCollect(orderline2, 1)

//    val orderline3 = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF().repartition(32)
//    orderline3.filter($"ol_delivery_d" >= 20071212)
//    logger.info("[MICRO_QUERY %s] Filter at compute level. Repartition [%d]: %s".format(1, 32, orderline3.printSchema))
//    timeCollect(orderline3, 1)

//    val orderline4 = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF().repartition(16)
//    orderline4.filter($"ol_delivery_d" >= 20071212)
//    logger.info("[MICRO_QUERY %s] Filter at compute level. Repartition [%d]: %s".format(1, 16, orderline3.printSchema))
//    timeCollect(orderline3, 1)

    scc.sparkContext.stop()
  }
}
