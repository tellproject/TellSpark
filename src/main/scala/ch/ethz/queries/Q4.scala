package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

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
    import sqlContext.implicits._
    //val tellRdd = new TellRDD[TellRecord](sc, "order", new ScanQuery(), CHSchema.orderLineSch)

    val orders = orderRdd(scc, new ScanQuery()).toDF()

    val orderline = orderLineRdd(scc, new ScanQuery()).toDF()
    /**
     * select *
		    from orderline
		    where o_id = ol_o_id
		    and o_w_id = ol_w_id
		    and o_d_id = ol_d_id
		    and ol_delivery_d >= o_entry_d
     */
    val forderline = orderline
      .filter(orderline("ol_delivery_d").geq(orders("o_entry_d")))
      .select($"ol_o_id", $"ol_w_id", $"ol_d_id").distinct

    val res = forderline.join(orders, ((orders("o_id") === $"ol_o_id") &&
      (orders("o_w_id") === $"ol_w_id") &&
      (orders("o_d_id") === $"ol_d_id")))

    timeCollect(res, 4)
  }
}
