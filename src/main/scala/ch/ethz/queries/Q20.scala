package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	 su_name, su_address
from	 supplier, nation
where	 su_suppkey in
		(select  mod(s_i_id * s_w_id, 10000)
		from     stock, orderline
		where    s_i_id in
				(select i_id
				 from item
				 where i_data like 'co%')
			 and ol_i_id=s_i_id
			 and ol_delivery_d > '2010-05-23 12:00:00'
		group by s_i_id, s_w_id, s_quantity
		having   2*s_quantity > sum(ol_quantity))
	 and su_nationkey = n_nationkey
	 and n_name = 'Germany'
order by su_name
 */
class Q20 extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    //select i_id from item where i_data like 'co%'
    val fitem = itemRdd(scc, new ScanQuery, ChTSchema.itemSch).toDF()
      .filter($"i_data".like("co%")).select($"i_id")
    val forderline = orderLineRdd(scc, new ScanQuery, ChTSchema.orderLineSch).toDF().filter($"ol_delivery_d" > 20100523)
    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val supplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
    val fnation = nationRdd(scc, new ScanQuery, ChTSchema.nationSch).toDF().filter($"n_name" === "Germany")

    val inner_query = stock
      .join(forderline, (forderline("ol_i_id")===$"s_i_id") && ($"s_i_id".isin(fitem("i_id")) ))//TODO do with a join with item?
      .groupBy($"s_i_id", $"s_w_id", $"s_quantity")
      .agg(sum($"ol_quantity").as("sum_qty"))
      .filter($"s_quantity".*(2) > $"sum_qty")
      .select((($"s_i_id" * $"s_w_id")%10000).as("inner_suppkey"))

    //TODO verify the <isin> operation
    val res = supplier.join(fnation, fnation("n_nationkey") === $"su_nationkey")
    .filter($"su_suppkey".isin(inner_query("inner_suppkey")))

    timeCollect(res, 20)
  }

}
