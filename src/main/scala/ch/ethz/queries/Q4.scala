package ch.ethz.queries

import ch.ethz.TellClientFactory
import ch.ethz.tell.{ScanQuery, TRecord, TRDD, TSparkContext}

/**
 * Query4
 * select	o_ol_cnt, count(*) as order_count
 * from	orders
where	o_entry_d >= '2007-01-02 00:00:00.000000'
	and o_entry_d < '2012-01-02 00:00:00.000000'
	and exists (select *
		    from orderline
		    where o_id = ol_o_id
		    and o_w_id = ol_w_id
		    and o_d_id = ol_d_id
		    and ol_delivery_d >= o_entry_d)
group	by o_ol_cnt
order	by o_ol_cnt
 */
class Q4 extends ChQuery {
  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    //val tellRdd = new TellRDD[TellRecord](sc, "order", new ScanQuery(), CHSchema.orderLineSch)

    val orders = orderRdd(scc, new ScanQuery()).toDF()

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
    /**
     * select *
		    from orderline
		    where o_id = ol_o_id
		    and o_w_id = ol_w_id
		    and o_d_id = ol_d_id
		    and ol_delivery_d >= o_entry_d
     */
    val forderline = orderline
      .filter(orderline("OL_DELIVERY_D").geq(orders("O_ENTRY_D")))
      .select($"OL_O_ID", $"OL_W_ID", $"OL_D_ID").distinct

    val res = forderline.join(orders, ((orders("O_ID") === $"OL_O_ID") &&
      (orders("O_W_ID") === $"OL_W_ID") &&
      (orders("O_D_ID") === $"OL_D_ID")))

    timeCollect(res, 4)
    //    //todo push down filter
    //    val orderRdd = new TRDD[TRecord](scc, "order", new ScanQuery(), ChTSchema.orderSch).filter(r => {
    //      val f1 = r.getValue("O_ENTR_D").asInstanceOf[Long] >= 20070102
    //      val f2 = r.getValue("O_ENTR_D").asInstanceOf[Long] < 20120102
    //      (f1&f2)
    //    }).map(r => {
    //      val key = (r.getValue("O_ID").asInstanceOf[Int], r.getValue("O_W_ID").asInstanceOf[Int], r.getValue("O_D_ID").asInstanceOf[Int])
    //      (key, r)
    //    })
    //
    //    val orderLineRdd = new TRDD[TRecord](scc, "order_line", new ScanQuery(), ChTSchema.orderLineSch).map(r => {
    //      val key = (r.getValue("OL_O_ID").asInstanceOf[Int], r.getValue("OL_W_ID").asInstanceOf[Int], r.getValue("OL_D_ID").asInstanceOf[Int])
    //      (key, key)
    //    })
    //    val o_ol = orderRdd.join(orderLineRdd)
    //    val cntOrOrLi = o_ol.count()
    //      if (cntOrOrLi > 0) {
    //      //TODO check count function
    //        val res = o_ol.map(r => (r._2._1.getValue("O_OL_CNT").asInstanceOf[Int], r._2._1.getValue("O_ID").asInstanceOf[Int]))
    //        .groupBy(g => g._1)
    //        .map(r => (r._1, r._2.count(_)))
    //        //result
    //        res.collect()
    //    }
  }
}
