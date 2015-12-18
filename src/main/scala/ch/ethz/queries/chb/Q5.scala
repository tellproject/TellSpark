package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell._
import org.apache.spark.sql.SQLContext

/**
 * Ch Query5
 *
 * select	 n_name,
 * sum(ol_amount) as revenue
 * from	 customer, orders, orderline, stock, supplier, nation, region
 * where	 c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
 * and ol_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_w_id = s_w_id and ol_i_id = s_i_id
 * and mod((s_w_id * s_i_id),10000) = su_suppkey
 * and ascii(substr(c_state,1,1)) = su_nationkey
 * and su_nationkey = n_nationkey
 * and n_regionkey = r_regionkey
 * and r_name = 'Europe'
 * and o_entry_d >= '2007-01-02 00:00:00.000000'
 * group by n_name
 * order by revenue desc
 */
class Q5 extends ChQuery {
  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._
//    println("[TELL] PARAMETERS USED: " + TClientFactory.toString())
//
//    // prepare date selection
//    val dateSelection = new CNFClause
//    dateSelection.addPredicate(
//      ScanQuery.CmpType.GREATER_EQUAL, oSchema.getField("o_entry_d").index, referenceDate2007)
    val ordQry = new TScanQuery("order", tSparkContext.partNum.value, Big)
//    orderQuery.addSelection(dateSelection)

    // prepare region selection (not sure whether that helps)
//    val regionSelection = new CNFClause
//    regionSelection.addPredicate(
//      ScanQuery.CmpType.EQUAL, rSchema.getField("r_name").index, new StringType("Europe"))
    val regionQuery = new TScanQuery("region", tSparkContext.partNum.value, Small)
//    regionQuery.addSelection(regionSelection)

    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val cusQry = new TScanQuery("customer", tSparkContext.partNum.value, Big)
    val supQry = new TScanQuery("supplier", tSparkContext.partNum.value, Small)
    val stkQry = new TScanQuery("stock", tSparkContext.partNum.value, Big)
    val natQry = new TScanQuery("nation", tSparkContext.partNum.value, Small)

    //orderline
    val orderline = orderLineRdd(tSparkContext, olQry, ChTSchema.orderLineSch).toDF()
    //customer
    val customer = customerRdd(tSparkContext, cusQry, ChTSchema.customerSch).toDF()
//     orders
    val orders = orderRdd(tSparkContext, ordQry, ChTSchema.orderSch).toDF()
    // stock
    val stock = stockRdd(tSparkContext, stkQry, ChTSchema.stockSch).toDF()
    //supplier
    val supplier = supplierRdd(tSparkContext, supQry, ChTSchema.supplierSch).toDF()
    //nation
    val nation = nationRdd(tSparkContext, natQry, ChTSchema.nationSch).toDF()
    //region
    val region = regionRdd(tSparkContext, regionQuery, ChTSchema.regionSch).toDF()

    val forder = orders
      .filter(orders("o_entry_d").geq(20070102))
    val fregion = region
      .filter(region("r_name").eqNullSafe("Europe"))
    val part_res = customer.join(forder, ($"c_id" === forder("o_c_id")) &&
      ($"c_w_id" === forder("o_w_id")) &&
      ($"c_d_id" === forder("o_d_id")))
    .join(orderline, (orderline("ol_o_id") === forder("o_id")) &&
      (orderline("ol_w_id") === forder("o_w_id")) &&
      (orderline("ol_d_id") === forder("o_d_id")))
    .join(stock, (orderline("ol_w_id") === stock("s_w_id")) &&
      (orderline("ol_i_id") === stock("s_i_id")) )
//
    val jsupp = supplier.join(nation, $"su_nationkey" === nation("n_nationkey"))
    .join(fregion, nation("n_regionkey") === region("r_regionkey"))
//
    val part_2 = part_res
      .join(jsupp, (part_res("s_w_id")*part_res("s_i_id")%10000 === jsupp("su_suppkey")) &&
      (part_res("c_state").substr(1,1).eq(jsupp("su_nationkey"))))
    //todo push down filter
    val res = part_2.groupBy(part_2("n_name"))
    .agg(sum($"ol_amount").as("revenue"))
    .orderBy("revenue")
    .select("n_name", "revenue")

    timeCollect(res, 5)
  }

}
