package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * Query3
 */
class Q3 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // convert an RDDs to a DataFrames
    val orderline = orderLineRdd(scc, new ScanQuery).toDF()
    val orders = orderRdd(scc, new ScanQuery).toDF()
    val new_order = newOrderRdd(scc, new ScanQuery).toDF()
    val customer = customerRdd(scc, new ScanQuery).toDF()
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
     customer.filter(customer("c_state").like("A%"))
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
    .select(orderline("OL_O_ID"), orderline("ol_w_id"), orderline("ol_d_id"), orders("o_entry_d"))
    .orderBy($"revenue".desc, orders("o_entry_d"))

//    res.collect()
    //    println("[TUPLES] %d".format(result.length))
  }
}
