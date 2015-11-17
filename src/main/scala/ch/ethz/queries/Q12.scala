package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	 o_ol_cnt,
	 sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
	 sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
from	 orders, orderline
where	 ol_w_id = o_w_id
	 and ol_d_id = o_d_id
	 and ol_o_id = o_id
	 and o_entry_d <= ol_delivery_d
	 and ol_delivery_d < '2020-01-01 00:00:00.000000'
group by o_ol_cnt
order by o_ol_cnt
 */
class Q12  extends ChQuery {

  override def execute(st: String, cm: String, cn:Int, cs:Int, mUrl:String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)
    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val high_line_count = udf { (x: Int) => if (x == 1 || x == 2) 1 else 0 }
    val low_line_count = udf { (x: Int) => if (x != 1 && x != 2) 1 else 0 }

    val oo = orderRdd(scc, new ScanQuery)
    val ol = orderLineRdd(scc, new ScanQuery)

    val orders = oo.toDF()
    val forderline = ol.toDF().filter($"ol_delivery_d" < 20200101)

    val res = orders.join(forderline, ($"ol_w_id" === forderline("o_w_id")) &&
      ($"ol_d_id" === forderline("o_d_id")) &&
      ($"ol_o_id" === forderline("o_id")) &&
      (forderline("o_entry_d") <= $"ol_delivery_d"))
    .select($"o_ol_cnt")
    .groupBy($"o_ol_cnt")
    .agg(sum(high_line_count($"o_carrier_id")), sum(low_line_count($"o_carrier_id")) )
    .orderBy($"o_ol_cnt")

    timeCollect(res, 12)
  }
}
