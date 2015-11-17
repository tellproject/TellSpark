package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	 c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
from	 customer, orders, orderline, nation
where	 c_id = o_c_id
	 and c_w_id = o_w_id
	 and c_d_id = o_d_id

	 and ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
 and o_entry_d <= ol_delivery_d

	 and o_entry_d >= '2007-01-02 00:00:00.000000'
	 and n_nationkey = ascii(substr(c_state,1,1))
group by c_id, c_last, c_city, c_phone, n_name
order by revenue desc
 */
class Q10  extends ChQuery {

  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String, chTSchema:ChTSchema): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val cc = customerRdd(scc, new ScanQuery, chTSchema.customerSch)
    val oo = orderRdd(scc, new ScanQuery, chTSchema.orderSch)
    val ol = orderLineRdd(scc, new ScanQuery, chTSchema.orderLineSch)
    val nn = nationRdd(scc, new ScanQuery, chTSchema.nationSch)

    val forderline = oo.toDF().filter($"o_entry_d" >= 20070102)
    val customer = cc.toDF()
    val nation = nn.toDF()
    val orders = oo.toDF()
    val c_n = customer.join(nation, $"c_state".substr(1,1) === nation("n_nationkey"))
    val o_ol = orders.join(forderline, (forderline("ol_w_id") === $"o_w_id" &&
      forderline("ol_d_id") === $"o_d_id" &&
      forderline("ol_o_id") === $"o_id" &&
      forderline("ol_delivery_d") >= $"o_entry_d" ) )
    val res = c_n.join(o_ol, ( (c_n("c_id") === o_ol("o_c_id")) &&
      (c_n("c_w_id") === o_ol("o_w_id")) &&
      (c_n("c_d_id") === o_ol("o_d_id")) ))
      //c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
    .select("c_id", "c_last", "c_city", "c_phone", "n_name")
    .agg(sum($"ol_amount").as("revenue"))
    .orderBy($"revenue".desc)

    timeCollect(res, 10)
  }

}
