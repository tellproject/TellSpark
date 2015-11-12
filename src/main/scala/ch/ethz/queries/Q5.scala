package ch.ethz.queries

import ch.ethz.TellClientFactory
import ch.ethz.tell.{TSparkContext, ScanQuery, TRecord, TRDD}

/**
 * Query5
 * select	 n_name,
	 sum(ol_amount) as revenue
from	 customer, orders, orderline, stock, supplier, nation, region
where	 c_id = o_c_id
	 and c_w_id = o_w_id
	 and c_d_id = o_d_id

	 and ol_o_id = o_id
	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id

	 and ol_w_id = s_w_id
	 and ol_i_id = s_i_id

	 and mod((s_w_id * s_i_id),10000) = su_suppkey
	 and ascii(substr(c_state,1,1)) = su_nationkey

	 and su_nationkey = n_nationkey

	 and n_regionkey = r_regionkey
	 and r_name = 'Europe'

	 and o_entry_d >= '2007-01-02 00:00:00.000000'
group by n_name
order by revenue desc
 */
class Q5 extends ChQuery {
  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String, appName: String): Unit = {
    val scc = new TSparkContext(mUrl, appName, st, cm, cn, cs)
    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())
    //todo push down filter
    val orderRdd = new TRDD[TRecord](scc, "order", new ScanQuery(), orderSch).filter(r => {
      r.getField("O_ENTR_D").asInstanceOf[Long] >= 20070102
    }).map(r => {
      // key(o_c_id, o_w_id, o_d_id) val(o_id)
      val key = (r.getField("O_C_ID").asInstanceOf[Int], r.getField("O_W_ID").asInstanceOf[Int], r.getField("O_D_ID").asInstanceOf[Int])
      (key, r.getField("O_ID").asInstanceOf[Int])
    })

    val customerRdd = new TRDD[TRecord](scc, "customer", new ScanQuery(), customerSch).map(r => {
      // key(c_id, c_w_id, c_d_id) val(c_state)
      val key = (r.getField("C_ID").asInstanceOf[Int], r.getField("C_W_ID").asInstanceOf[Int], r.getField("C_D_ID").asInstanceOf[Int])
      (key, r.getField("C_STATE").asInstanceOf[String])
    })

    val orderlineRdd = new TRDD[TRecord](scc, "order_line", new ScanQuery(), orderLineSch)
    val stockRdd = new TRDD[TRecord](scc, "stock", new ScanQuery(), stockSch)
    val supplierRdd = new TRDD[TRecord](scc, "supplier", new ScanQuery(), supplierSch)
    val nationRdd = new TRDD[TRecord](scc, "nation", new ScanQuery(), nationSch)
    val regionRdd = new TRDD[TRecord](scc, "region", new ScanQuery(), regionSch)

    val orderlineMapped = orderlineRdd.map(r => {
      //key(ol_o_id, ol_w_id, ol_d_id) val(orderline)
      val key = (r.getField("OL_O_ID").asInstanceOf[Int], r.getField("OL_W_ID").asInstanceOf[Int], r.getField("OL_D_ID").asInstanceOf[Int])
      (key, r)
    })

    //c_id = o_c_id  and c_w_id = o_w_id  and c_d_id = o_d_id
    val cust_order = customerRdd.join(orderRdd).map(r => {
      // key (o_id, o_w_id, o_d_id) value(c_state)
      val key = (r._2._2, r._1._2, r._1._3)
      (key, r._2._1)
    })
    //ol_o_id = o_id  and ol_w_id = o_w_id  and ol_d_id = o_d_id
    val cust_order_orderline = cust_order.join(orderlineMapped).map(r => {
      // key(ol_w_id, ol_i_id) val(c_state)
      val key = (r._1._2, r._1._3)
      (key, r._2._1)
    })
    // ol_w_id = s_w_id and ol_i_id = s_i_id
    val orderline2 = orderlineRdd.map(r => {
      // key (ol_w_id, ol_i_id) val(ol_amount)
      val key =(r.getField("OL_W_ID").asInstanceOf[Int],r.getField("OL_I_ID").asInstanceOf[Int])
      (key, r.getField("OL_AMOUNT").asInstanceOf[Double])
    })
    val orderline2_stock = orderline2.join(stockRdd.map(r => {
      val key = (r.getField("S_W_ID").asInstanceOf[Int], r.getField("S_I_ID").asInstanceOf[Int])
      (key, key)
    }))
    //ol_w_id = s_w_id and ol_i_id = s_i_id
    val c_o_ol_s = cust_order_orderline.join(orderline2_stock)

    // mod((s_w_id * s_i_id),10000) = su_suppkey and ascii(substr(c_state,1,1)) = su_nationkey
    val c_o_ol_s_su_s = c_o_ol_s.map(r => {
      //key(susbstring(c_state,1,1)) val(ol_amount)
      (r._2._1.substring(1,1), r._2._2._1)
      })
      .join(supplierRdd.map(r => { (r.getField("SU_NATIONKEY").asInstanceOf[String], r.getField("SU_SUPPKEY").asInstanceOf[Int])}))
      .map(r => {
      //key(su_suppkey) val(su_nationkey, ol_amount)
        (r._2._2, (r._1, r._2._1))
      })
      .join(stockRdd.map(r=> {
      //mod((s_w_id * s_i_id),10000)
        //key(s_w_id) val(s_w_id)
        val key = (r.getField("s_w_id").asInstanceOf[Int] *r.getField("s_i_id").asInstanceOf[Int]) % 10000
        (key, key)
      }))
    //su_nationkey = n_nationkey
    val partial_res = c_o_ol_s_su_s.map(r => (r._2._1._1, r._2._1._2))
      .join(nationRdd.map(r => (r.getField("n_nationkey").asInstanceOf[String], (r.getField("n_regionkey").asInstanceOf[Int], r.getField("n_name").asInstanceOf[String]))))
      .map(r => {(r._2._2._1, (r._2._1, r._2._2._2))})
      .join(regionRdd.map(r => (r.getField("r_regionkey").asInstanceOf[Int], r.getField("r_regionkey").asInstanceOf[Int])))

    val res = partial_res.groupBy(r => r._2._1._2).map(r => {
      val key = r._1
      var aggr = 0.0
      for (x <- r._2) {
        aggr += x._2._1._1
      }
      (key, aggr)
    })

    res.collect()

  }

}
