package ch.ethz.queries

import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

/**
 * Query4
 */
class Q4 extends ChQuery {
  /**
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
  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    // prepare date selection
    val oSchema = ChTSchema.orderSch
    val orderQuery = new ScanQuery
    val oEntryIndex = oSchema.getField("o_entry_d").index

    val dateSelectionLower = new CNFClause
    dateSelectionLower.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oEntryIndex, referenceDate2007)
    orderQuery.addSelection(dateSelectionLower)

    val dateSelectionUpper = new CNFClause
    dateSelectionUpper.addPredicate(
      ScanQuery.CmpType.LESS, oEntryIndex, referenceDate2012)
//    orderQuery.addSelection(dateSelectionUpper)

    val oo = orderRdd(scc, orderQuery, oSchema)
//    if (logger.isDebugEnabled) {
      logger.debug("[Query %s] Tuples:%d".format(this.className, oo.count))
//    }
      val orders = oo.toDF()

    val orderline = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF()
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
    scc.sparkContext.stop()
  }
}
