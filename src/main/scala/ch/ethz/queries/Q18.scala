package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	 c_last, c_id o_id, o_entry_d, o_ol_cnt, sum(ol_amount)
from	 customer, orders, orderline
where	 c_id = o_c_id
	 and c_w_id = o_w_id
	 and c_d_id = o_d_id
	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
group by o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt
having	 sum(ol_amount) > 200
order by sum(ol_amount) desc, o_entry_d
 */
class Q18  extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String, chTSchema:ChTSchema): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val orderline = orderLineRdd(scc, new ScanQuery, chTSchema.orderLineSch).toDF()
    val orders = orderRdd(scc, new ScanQuery, chTSchema.orderSch).toDF()
    val customer = customerRdd(scc, new ScanQuery, chTSchema.customerSch).toDF()

    val res = customer.join(orders, $"c_id" === orders("o_c_id") &&
      $"c_w_id" === orders("o_w_id") &&
      $"c_d_id" === orders("o_d_id"))
    .join(orderline, orderline("ol_w_id") === $"o_w_id" &&
      orderline("ol_d_id") === $"o_d_id" &&
      orderline("ol_o_id") === $"o_id")
    .select("c_last", "c_id", "o_id", "o_entry_d", "o_ol_cnt")
    .agg(sum($"ol_amount").as("tot_amount"))
    .filter($"tot_amount" > 200)
    .orderBy($"tot_amount".desc, $"o_entry_d")

    timeCollect(res, 18)
  }

}
