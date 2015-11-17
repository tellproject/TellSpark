package ch.ethz.queries

import ch.ethz.tell.{ScanQuery, TSparkContext}

/**
 * select	 i_name,
	 substr(i_data, 1, 3) as brand,
	 i_price,
	 count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt
from	 stock, item
where	 i_id = s_i_id
	 and i_data not like 'zz%'
	 and (mod((s_w_id * s_i_id),10000) not in
		(select su_suppkey
		 from supplier
		 where su_comment like '%bad%'))
group by i_name, substr(i_data, 1, 3), i_price
order by supplier_cnt desc
 */
class Q16  extends ChQuery {

  /**
   * implemented in children classes and hold the actual query
   */
  override def execute(st: String, cm: String, cn: Int, cs: Int, mUrl: String): Unit = {
    val scc = new TSparkContext(mUrl, className, st, cm, cn, cs)

    val sqlContext = new org.apache.spark.sql.SQLContext(scc.sparkContext)
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val orders = orderRdd(scc, new ScanQuery, ChTSchema.orderSch).toDF()
    val fsupplier = supplierRdd(scc, new ScanQuery, ChTSchema.supplierSch).toDF()
      .filter($"su_comment".like("%bad%"))
//      .select($"su_suppkey")

    val fitem = itemRdd(scc, new ScanQuery, ChTSchema.itemSch).toDF()
    .filter(!$"i_data".like("zz%"))

    val stock = stockRdd(scc, new ScanQuery, ChTSchema.stockSch).toDF()
    val res = stock.join(fsupplier, ( ($"s_w_id" * $"s_i_id")%10000 !== (fsupplier("su_suppkey")) ))
    .join(fitem, $"i_id" === $"s_i_id")
    .select($"i_name",
        $"i_data".substr(1, 3).as("brand"),
        $"i_price,",
        countDistinct(($"s_w_id" * $"s_i_id")%10000).as("supplier_cnt"))
    //TODO double check other queries with similar structure
    timeCollect(res, 16)
  }

}
