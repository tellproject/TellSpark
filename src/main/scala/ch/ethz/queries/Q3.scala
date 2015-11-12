package ch.ethz.queries

import ch.ethz.TellClientFactory
import ch.ethz.tell.{ScanQuery, TRecord, TRDD, TSparkContext}

/**
 * Query3
 * select ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) as revenue, o_entry_d
 * from customer, neworder, orders, orderline
 * where c_state like 'A%' and c_id = o_c_id and c_w_id = o_w_id
 * and c_d_id = o_d_id and no_w_id = o_w_id and no_d_id = o_d_id
 * and no_o_id = o_id and ol_w_id = o_w_id and ol_d_id = o_d_id
 * and ol_o_id = o_id and o_entry_d > '2007-01-02 00:00:00.000000'
 * group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
 * order by revenue desc, o_entry_d
 */
class Q3 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String, appName:String): Unit = {
    val scc = new TSparkContext(mUrl, appName, st, cm, cn, cs)
    println("[TELL] PARAMETERS USED: " + TellClientFactory.toString())

    //TODO push-downs
    val customerRdd = new TRDD[TRecord](scc, "customer", new ScanQuery(), customerSch).filter(r => {
      r.getField("C_STATE").asInstanceOf[String].contains("A")
    })
    //TODO projections
    val newOrderRdd = new TRDD[TRecord](scc, "new_order", new ScanQuery(), newOrderSch).map(r => {
      val key = (r.getField("NO_W_ID").asInstanceOf[Int], r.getField("NO_D_ID").asInstanceOf[Int], r.getField("NO_I_ID").asInstanceOf[Int])
      (key, r)
    })
    //TODO push-downs
    val orderRdd = new TRDD[TRecord](scc, "orders", new ScanQuery(), orderSch).filter(r => {
      r.getField("O_ENTRY_D").asInstanceOf[Int] > 20070102
    })
    //TODO projections
    val orderlineRdd = new TRDD[TRecord](scc, "order_line", new ScanQuery(), orderLineSch).map(r => {
      val key = (r.getField("OL_W_ID").asInstanceOf[Int], r.getField("OL_D_ID").asInstanceOf[Int], r.getField("OL_O_ID").asInstanceOf[Int])
      (key, r)
    })

    // c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id
    val order4cust = orderRdd.map(r => {
      val key = (r.getField("O_C_ID").asInstanceOf[Int], r.getField("O_W_ID").asInstanceOf[Int], r.getField("O_D_ID").asInstanceOf[Int])
      (key, r)
    })
    val custJOrder = customerRdd.map(r => {
      val key = (r.getField("C_ID").asInstanceOf[Int], r.getField("C_W_ID").asInstanceOf[Int], r.getField("C_D_ID").asInstanceOf[Int])
      (key, r)
    }).join(order4cust).map(r => {
      val oRec = r._2._2
      val key = (oRec.getField("O_W_ID").asInstanceOf[Int], oRec.getField("O_D_ID").asInstanceOf[Int], oRec.getField("O_ID").asInstanceOf[Int])
      (key, oRec)
    })

    // no_w_id = o_w_id and no_d_id = o_d_id and no_o_id = o_id
    val custJorderJnew = custJOrder.join(newOrderRdd).map(r => (r._1, r._2._1))
    // ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
    val custJorderJnewJolgr = custJorderJnew.join(orderlineRdd).map(r => {
      val k = (r._1._1, r._1._2, r._1._3, r._2._1.getField("O_ENTRY_D").asInstanceOf[Int])
      (k, r._2._2)
    }).groupBy(r => r._1)

    //ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) as revenue, o_entry_d
    val res = custJorderJnewJolgr.map(r => {
      var aggSum = 0.0
      for (x <- r._2) {
        aggSum += x._2.getField("OL_AMOUNT").asInstanceOf[Double]
      }
      (r._1, aggSum)
    })
    res.collect()
    //    println("[TUPLES] %d".format(result.length))
  }
}
