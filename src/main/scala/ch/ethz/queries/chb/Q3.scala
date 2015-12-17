package ch.ethz.queries.chb

import ch.ethz.TScanQuery
import ch.ethz.queries.ChQuery
import ch.ethz.tell.{BufferType, CNFClause, ScanQuery, TSparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Ch Query3
 */
class Q3 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(tSparkContext: TSparkContext, sqlContext: SQLContext): Unit = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    import BufferType._


    // prepare date selection
    val oSchema = ChTSchema.orderSch
    val dateSelection = new CNFClause
    dateSelection.addPredicate(ScanQuery.CmpType.GREATER, oSchema.getField("o_entry_d").index, referenceDate2007)
    val orderQuery = new TScanQuery("order", tSparkContext.partNum.value, Big)
//    orderQuery.addSelection(dateSelection)
//
    // prepare c_state selection
    val cSchema = ChTSchema.customerSch
//    val stateSelection = new CNFClause
//    stateSelection.addPredicate(
//      ScanQuery.CmpType.LIKE, cSchema.getField("c_state").index, new StringType("A%"))
    val cusQry = new TScanQuery("customer", tSparkContext.partNum.value, Big)
    val olQry = new TScanQuery("order-line", tSparkContext.partNum.value, Big)
    val noQry = new TScanQuery("new-order", tSparkContext.partNum.value, Big)
//    customerQuery.addSelection(stateSelection)
//
//    // convert an RDDs to a DataFrames
    val orderline = orderLineRdd(tSparkContext, olQry, ChTSchema.orderLineSch).toDF()
    val orders = orderRdd(tSparkContext, orderQuery, oSchema).toDF()
    val new_order = newOrderRdd(tSparkContext, noQry, ChTSchema.newOrderSch).toDF()
    val customer = customerRdd(tSparkContext, cusQry, cSchema).toDF()
    /**
     *  * select ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) as revenue, o_entry_d
     * from customer, neworder, orders, orderline
     * where c_state like 'A%' and c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
     * and no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id
     * and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
     * and o_entry_d > '2007-01-02 00:00:00.000000'
     * group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
     * order by revenue desc, o_entry_d
     */
     val res = customer
       .filter(customer("c_state").like("A%"))
       .join(orders, (($"c_id" === orders("o_c_id")) &&
       ($"c_w_id" === orders("o_w_id")) &&
       ($"c_d_id" === orders("o_d_id"))))
    .join(new_order, ($"o_w_id" === new_order("no_w_id")) &&
       ($"o_d_id" === new_order("no_d_id")) &&
       ($"o_id" === new_order("no_o_id")))
    .join(orderline, ($"o_w_id" === orderline("ol_w_id")) &&
       ($"o_d_id" === orderline("ol_d_id")) &&
       ($"o_id" === orderline("ol_o_id")))
    .filter(orders("o_entry_d") > 20070102)
    .groupBy(orderline("ol_o_id"), orderline("ol_w_id"), orderline("ol_d_id"), orders("o_entry_d"))
    .agg(sum($"ol_amount").as("revenue"))
    .select(orderline("ol_o_id"), orderline("ol_w_id"), orderline("ol_d_id"), orders("o_entry_d"))
    .orderBy($"revenue".desc, orders("o_entry_d"))
//
    timeCollect(res, 3)

  }
}