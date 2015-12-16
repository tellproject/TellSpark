package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell._
import org.apache.spark.sql.SQLContext

/**
 * Ch Query1
 *
 * select ol_number, sum(ol_quantity) as sum_qty, sum(ol_amount) as sum_amount,
 * avg(ol_quantity) as avg_qty, avg(ol_amount) as avg_amount, count(*) as count_order
 * from orderline where ol_delivery_d > '2007-01-02 00:00:00.000000'
 * group by ol_number order by ol_number
 */
class Q1 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {

    import BufferType._
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val oSchema = ChTSchema.orderLineSch

    // prepare date selection
    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.GREATER, oSchema.getField("ol_delivery_d").index, referenceDate2007)
    val orderLineQuery = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    orderLineQuery.addSelection(dateSelection)

    //val orderline = orderLineRdd(tSparkContext, orderLineQuery, ChTSchema.orderLineSch)
    //println(orderline.count)
    val orderline = orderLineRdd(tSparkContext, orderLineQuery, ChTSchema.orderLineSch).toDF()
    logger.info("[Query %d] %s".format(1, orderline.printSchema))

    //ToDo projection push downs
    val res = orderline
      .groupBy($"ol_number")
      .agg(sum($"ol_amount"),
        sum($"ol_quantity"),
        avg($"ol_quantity"),
        avg($"ol_amount"),
        count($"ol_number"))
      .sort($"ol_number")

    timeCollect(res, 1)
  }
}
