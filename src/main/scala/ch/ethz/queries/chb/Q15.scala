package ch.ethz.queries.chb

import ch.ethz.queries.ChQuery
import ch.ethz.tell.{CNFClause, ScanQuery, TSparkContext}

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
  override def execute(st: String, cm: String, cn: Int, cs: Long, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    // prepare date selection
    val olSchema = ChTSchema.orderLineSch
    val orderLineQuery = new ScanQuery
    val oDeliveryIndex = olSchema.getField("ol_delivery_d").index

    val dateSelection = new CNFClause
    dateSelection.addPredicate(
      ScanQuery.CmpType.GREATER_EQUAL, oDeliveryIndex, referenceDate2007)
    orderLineQuery.addSelection(dateSelection)

    val forderline = orderLineRdd(scc, orderLineQuery, olSchema).toDF()
//      .filter($"ol_delivery_d" >= 20070102)
    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val supplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()

    val revenue = forderline.join(stock, ($"ol_i_id" === stock("s_i_id") && $"ol_supply_w_id" === stock("s_w_id")))
      .select((($"s_w_id" * $"s_i_id")%10000).as("supplier_no"), $"ol_amount")
    .groupBy($"supplier_no")
    .agg(sum($"ol_amount").as("total_revenue"))
    .select($"total_revenue", $"supplier_no")

    val max_revenue = revenue.select($"total_revenue").agg(max($"total_revenue").as("total_revenue"))
    val res = supplier.join(revenue, $"su_suppkey" === revenue("supplier_no"))
    //.filter(revenue("total_revenue") === max_revenue("total_revenue"))
    .join(max_revenue, revenue("total_revenue") === max_revenue("total_revenue"))
    .orderBy($"su_suppkey")

    timeCollect(res, 15)
    scc.sparkContext.stop()
  }

}
