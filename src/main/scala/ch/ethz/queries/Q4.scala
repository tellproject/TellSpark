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
class Q4 extends ChQuery{
  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String, appName: String): Unit = {
    val scc = new TSparkContext(mUrl, appName, st, cm, cn, cs)
    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())
    //val tellRdd = new TellRDD[TellRecord](sc, "order", new ScanQuery(), CHSchema.orderLineSch)
    //todo push down filter
    val orderRdd = new TRDD[TRecord](scc, "order", new ScanQuery(), orderSch).filter(r => {
      val f1 = r.getField("O_ENTR_D").asInstanceOf[Long] >= 20070102
      val f2 = r.getField("O_ENTR_D").asInstanceOf[Long] < 20120102
      (f1&f2)
    }).map(r => {
      val key = (r.getField("O_ID").asInstanceOf[Int], r.getField("O_W_ID").asInstanceOf[Int], r.getField("O_D_ID").asInstanceOf[Int])
      (key, r)
    })

    val orderLineRdd = new TRDD[TRecord](scc, "order_line", new ScanQuery(), orderLineSch).map(r => {
      val key = (r.getField("OL_O_ID").asInstanceOf[Int], r.getField("OL_W_ID").asInstanceOf[Int], r.getField("OL_D_ID").asInstanceOf[Int])
      (key, key)
    })
    val o_ol = orderRdd.join(orderLineRdd)
    val cntOrOrLi = o_ol.count()
      if (cntOrOrLi > 0) {
      //TODO check count function
        val res = o_ol.map(r => (r._2._1.getField("O_OL_CNT").asInstanceOf[Int], r._2._1.getField("O_ID").asInstanceOf[Int]))
        .groupBy(g => g._1)
        .map(r => (r._1, r._2.count(_)))
        //result
        res.collect()
    }
  }
}
