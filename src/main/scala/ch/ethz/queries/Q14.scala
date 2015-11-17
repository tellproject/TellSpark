package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue
from	orderline, item
where	ol_i_id = i_id and ol_delivery_d >= '2007-01-02 00:00:00.000000'
	and ol_delivery_d < '2020-01-02 00:00:00.000000'
 */
class Q14 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val promo = udf { (x: String, y: Double) => if (x.startsWith("PR")) y else 0 }

    val ol = orderLineRdd(scc, new ScanQuery())
    val it = itemRdd(scc, new ScanQuery())
    val forderline = ol.toDF().filter($"ol_delivery_d" >= 20070102 && $"ol_delivery_d" < 20200102)
    val item = it.toDF()
    val res = forderline.join(item, $"ol_i_id" === item("i_id"))
      .agg(sum(promo($"i_data", $"ol_amount")) * 100 / (sum($"ol_amount").+(1))).as("promo_revenue")
    timeCollect(res, 14)
  }

}