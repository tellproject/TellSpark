package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TRecord, TRDD, TSparkContext}

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
    val orderline = new TRDD[TRecord](scc, "orderline", new ScanQuery(), ChTSchema.orderLineSch).map(r => {
      OrderLine(r.getValue("OL_O_ID").asInstanceOf[Int],
        r.getValue("OL_D_ID").asInstanceOf[Short],
        r.getValue("OL_W_ID").asInstanceOf[Int],
        r.getValue("OL_NUMBER").asInstanceOf[Short],
        r.getValue("OL_I_ID").asInstanceOf[Int],
        r.getValue("OL_SUPPLY_W_ID").asInstanceOf[Int],
        r.getValue("OL_DELIVERY_D").asInstanceOf[Long],
        r.getValue("OL_QUANTITY").asInstanceOf[Short],
        r.getValue("OL_AMOUNT").asInstanceOf[Long],
        r.getValue("OL_DIST_INFO").asInstanceOf[String]
      )
    }).toDF()


    val orders = new TRDD[TRecord](scc, "orders", new ScanQuery(), ChTSchema.orderSch).map(r => {
      Order(r.getValue("O_ID").asInstanceOf[Int],
        r.getValue("O_D_ID").asInstanceOf[Short],
        r.getValue("O_W_ID").asInstanceOf[Int],
        r.getValue("O_C_ID").asInstanceOf[Short],
        r.getValue("O_ENTRY_D").asInstanceOf[Long],
        r.getValue("O_CARRIER_ID").asInstanceOf[Short],
        r.getValue("O_OL_CNT").asInstanceOf[Short],
        r.getValue("O_ALL_LOCAL").asInstanceOf[Short]
      )
    }).toDF()

    val new_order = new TRDD[TRecord](scc, "new_order", new ScanQuery(), ChTSchema.newOrderSch).map(r => {
      NewOrder(r.getValue("NO_O_ID").asInstanceOf[Int],
        r.getValue("NO_D_ID").asInstanceOf[Short],
        r.getValue("NO_W_ID").asInstanceOf[Int]
      )
    }).toDF()

    val customer = new TRDD[TRecord](scc, "customer", new ScanQuery(), ChTSchema.customerSch).map(r => {
      Customer(r.getValue("C_ID").asInstanceOf[Int],
        r.getValue("C_D_ID").asInstanceOf[Int],
        r.getValue("C_W_ID").asInstanceOf[Int],
        r.getValue("C_FIRST").asInstanceOf[String],
        r.getValue("C_MIDDLE").asInstanceOf[String],
        r.getValue("C_LAST").asInstanceOf[String],
        r.getValue("C_STREET_1").asInstanceOf[String],
        r.getValue("C_STREET_2").asInstanceOf[String],
        r.getValue("C_CITY").asInstanceOf[String],
        r.getValue("C_STATE").asInstanceOf[String],
        r.getValue("C_ZIP").asInstanceOf[String],
        r.getValue("C_PHONE").asInstanceOf[String],
        r.getValue("C_SINCE").asInstanceOf[Long],
        r.getValue("C_CREDIT").asInstanceOf[String],
        r.getValue("C_CREDIT_LIM").asInstanceOf[Double],
        r.getValue("C_DISCOUNT").asInstanceOf[Double],
        r.getValue("C_BALANCE").asInstanceOf[Double],
        r.getValue("C_YTD_PAYMENT").asInstanceOf[Double],
        r.getValue("C_PAYMENT_CNT").asInstanceOf[Short],
        r.getValue("C_DELIVERY_CNT").asInstanceOf[Short],
        r.getValue("C_DATA").asInstanceOf[String],
        r.getValue("C_N_NATIONKEY").asInstanceOf[Int]
      )
    }).toDF()

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
     customer.filter(customer("C_STATE").like("A%"))
       .join(orders, (($"C_ID" === orders("O_C_ID")) &&
       ($"C_W_ID" === orders("O_W_ID")) &&
       ($"C_D_ID" === orders("O_D_ID"))))
    .join(new_order, ($"O_W_ID" === new_order("NO_W_ID")) &&
       ($"O_D_ID" === new_order("NO_D_ID")) &&
       ($"O_ID" === new_order("NO_O_ID")))
    .join(orderline, ($"o_w_id" === orderline("ol_w_id")) &&
       ($"o_d_id" === orderline("ol_d_id")) &&
       ($"o_id" === orderline("ol_o_id")))
    .filter(orders("o_entry_d") > 20070102)
    .groupBy(orderline("OL_O_ID"), orderline("OL_W_ID"), orderline("OL_D_ID"), orders("O_ENTRY_D"))
    .agg(sum($"OL_AMOUNT").as("REVENUE"))
    .select(orderline("OL_O_ID"), orderline("OL_W_ID"), orderline("OL_D_ID"), orders("O_ENTRY_D"))
    .orderBy($"REVENUE".desc, orders("O_ENTRY_D"))

//    res.collect()
    //    println("[TUPLES] %d".format(result.length))
  }
}
