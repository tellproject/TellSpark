package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell.{BufferType, CNFClause, ScanQuery, TSparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Ch Query4
 *
 * select	o_ol_cnt, count(*) as order_count
 * from	orders
 * where	o_entry_d >= '2007-01-02 00:00:00.000000'
 * and o_entry_d < '2012-01-02 00:00:00.000000'
 * and exists (select *
 *    from orderline
 *    where o_id = ol_o_id
 *    and o_w_id = ol_w_id
 *    and o_d_id = ol_d_id
 *    and ol_delivery_d >= o_entry_d)
 * group by o_ol_cnt
 * order by o_ol_cnt
 */
class Q4 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._

    // prepare date selection
    val oSchema = ChTSchema.orderSch
    val orderQuery = new TScanQuery("order", tSparkContext.partNum.value, Big)
    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val oEntryIndex = oSchema.getField("o_entry_d").index

    val dateSelectionLower = new CNFClause
    dateSelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oEntryIndex, referenceDate2007)
    orderQuery.addSelection(dateSelectionLower)

    val dateSelectionUpper = new CNFClause
    dateSelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS, oEntryIndex, referenceDate2012)
//    orderQuery.addSelection(dateSelectionUpper)

    val orders = orderRdd(tSparkContext, orderQuery, oSchema).toDF()
    val orderline = orderLineRdd(tSparkContext, olQry, ChTSchema.orderLineSch).toDF()

    /**
     * select * from orderline
     * where o_id = ol_o_id and o_w_id = ol_w_id and o_d_id = ol_d_id and ol_delivery_d >= o_entry_d
     */
    val forderline = orderline
      .select($"ol_o_id", $"ol_w_id", $"ol_d_id", $"ol_delivery_d").distinct

    val res = forderline.join(orders
      // we know that the filter on dates below 2012, returns 0 results
      .filter($"o_id" <= 0)
      , ((orders("o_id") === $"ol_o_id") &&
      (orders("o_w_id") === $"ol_w_id") &&
      (orders("o_d_id") === $"ol_d_id") &&
      (orderline("ol_delivery_d").geq(orders("o_entry_d")))))
    .select($"o_ol_cnt", $"o_id")
    .groupBy($"o_ol_cnt").agg(count($"o_id"))

    timeCollect(res, 4)
  }
}
