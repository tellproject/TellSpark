package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * with	 revenue (supplier_no, total_revenue) as (
	 select	mod((s_w_id * s_i_id),10000) as supplier_no,
		sum(ol_amount) as total_revenue
	 from	orderline, stock
		where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
		and ol_delivery_d >= '2007-01-02 00:00:00.000000'
	 group by mod((s_w_id * s_i_id),10000))

select	 su_suppkey, su_name, su_address, su_phone, total_revenue
from	 supplier, revenue
where	 su_suppkey = supplier_no
	 and total_revenue = (select max(total_revenue) from revenue)
order by su_suppkey
 */
class Q15  extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val forderline = orderLineRdd(scc, new ScanQuery).toDF().filter($"ol_delivery_d" >= 20070102)
    val stock = stockRdd(scc, new ScanQuery).toDF()
    val supplier = supplierRdd(scc, new ScanQuery).toDF()

    val revenue = forderline.join(stock, ($"ol_i_id" === stock("s_i_id") && $"ol_supply_w_id" === stock("s_w_id")))
      .select((($"s_w_id" * $"s_i_id")%10000).as("supplier_no"))
    .groupBy((($"s_w_id" * $"s_i_id")%10000))
    .agg(sum($"ol_amount").as("total_revenue"))

    val res = supplier.join(revenue, $"su_suppkey" === revenue("supplier_no"))
    .filter(revenue("total_revenue") === max(revenue("total_revenue")))
    .orderBy($"su_suppkey")

    timeCollect(res, 15)
  }

}
